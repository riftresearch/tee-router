use crate::{
    models::MarketOrderKind,
    models::{ProviderAddressRole, ProviderOperationStatus, ProviderOperationType},
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        across_client::{
            parse_optional_u256, AcrossClient, AcrossClientError, AcrossDepositStatusRequest,
            AcrossSwapApprovalRequest, AcrossTransaction,
        },
        asset_registry::{AssetRegistry, ProviderAssetCapability, ProviderId},
        custody_action_executor::{
            ChainCall, CustodyAction, CustodyActionReceipt, EvmCall, HyperliquidCall,
            HyperliquidCallNetwork, HyperliquidCallPayload,
        },
    },
};
use alloy::{
    hex,
    primitives::{Address, Bytes, FixedBytes, U256},
    sol,
    sol_types::{SolCall, SolEvent},
};
use chrono::{DateTime, TimeZone, Utc};
use hyperliquid_client::{
    actions::{Actions as HyperliquidActions, BulkOrder, Limit, Order, OrderRequest, Tif},
    bridge::{bridge_address as hyperliquid_bridge_address, minimum_bridge_deposit},
    client::Network as HyperliquidApiNetwork,
    info::{
        ClearinghouseState, L2BookSnapshot, L2Level, OrderStatusResponse, SpotClearinghouseState,
    },
    meta::{SpotMeta, TokenInfo, SPOT_ASSET_INDEX_OFFSET},
    HttpClient as HyperliquidHttpClient,
};
use hyperunit_client::{
    HyperUnitClient, UnitAsset, UnitChain, UnitGenerateAddressRequest, UnitOperation,
    UnitOperationState, UnitOperationsRequest,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::Arc,
};
use tokio::sync::OnceCell;
use uuid::Uuid;

const ACROSS_INTEGRATOR_ID: &str = "rift-router";
const MOCK_ACROSS_API_KEY: &str = "mock-across-api-key";
const VELORA_DEFAULT_PARTNER: &str = "rift-router";
const VELORA_NATIVE_TOKEN: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const VELORA_API_VERSION: &str = "6.2";
const VELORA_DEFAULT_SLIPPAGE_BPS: u64 = 100;
const CCTP_TOKEN_MESSENGER_V2_ADDRESS: &str = "0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d";
const CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS: &str = "0x81D40F21F12A8F0E3252Bccb954D722d4c464B64";
const MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS: &str = "0xcccccccccccccccccccccccccccccccccccc0001";
const MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS: &str = "0xcccccccccccccccccccccccccccccccccccc0002";
const CCTP_STANDARD_FINALITY_THRESHOLD: u32 = 2_000;

// Canonical Across V3 `FundsDeposited` event — field order, types, and indexed
// topics match the real SpokePool ABI so logs emitted by real Across (and by
// `MockSpokePool` in devnet) decode through the same path.
sol! {
    event FundsDeposited(
        bytes32 inputToken,
        bytes32 outputToken,
        uint256 inputAmount,
        uint256 outputAmount,
        uint256 indexed destinationChainId,
        uint256 indexed depositId,
        uint32 quoteTimestamp,
        uint32 fillDeadline,
        uint32 exclusivityDeadline,
        bytes32 indexed depositor,
        bytes32 recipient,
        bytes32 exclusiveRelayer,
        bytes message
    );
}

sol! {
    interface IERC20Approval {
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

sol! {
    interface ICircleTokenMessengerV2 {
        function depositForBurn(
            uint256 amount,
            uint32 destinationDomain,
            bytes32 mintRecipient,
            address burnToken,
            bytes32 destinationCaller,
            uint256 maxFee,
            uint32 minFinalityThreshold
        ) external returns (uint64);
    }
}

sol! {
    interface ICircleMessageTransmitterV2 {
        function receiveMessage(bytes calldata message, bytes calldata attestation) external returns (bool);
    }
}

pub type ProviderResult<T> = Result<T, String>;
pub type ProviderFuture<'a, T> = Pin<Box<dyn Future<Output = ProviderResult<T>> + Send + 'a>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeQuoteRequest {
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    #[serde(flatten)]
    pub order_kind: MarketOrderKind,
    pub recipient_address: String,
    pub depositor_address: String,
    pub partial_fills_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeQuote {
    pub provider_id: String,
    pub amount_in: String,
    pub amount_out: String,
    pub provider_quote: Value,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum BridgeExecutionRequest {
    Across(AcrossExecuteStepRequest),
    CctpBurn(CctpBurnStepRequest),
    CctpReceive(CctpReceiveStepRequest),
    HyperliquidBridgeDeposit(HyperliquidBridgeDepositStepRequest),
    HyperliquidBridgeWithdrawal(HyperliquidBridgeWithdrawalStepRequest),
}

impl BridgeExecutionRequest {
    pub fn across_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "across step request").map(Self::Across)
    }

    pub fn cctp_burn_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "cctp burn step request").map(Self::CctpBurn)
    }

    pub fn cctp_receive_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "cctp receive step request").map(Self::CctpReceive)
    }

    pub fn hyperliquid_bridge_deposit_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "hyperliquid bridge step request")
            .map(Self::HyperliquidBridgeDeposit)
    }

    pub fn hyperliquid_bridge_withdrawal_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "hyperliquid bridge withdrawal step request")
            .map(Self::HyperliquidBridgeWithdrawal)
    }

    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Across(_) => "across",
            Self::CctpBurn(_) => "cctp_burn",
            Self::CctpReceive(_) => "cctp_receive",
            Self::HyperliquidBridgeDeposit(_) => "hyperliquid_bridge_deposit",
            Self::HyperliquidBridgeWithdrawal(_) => "hyperliquid_bridge_withdrawal",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeQuoteRequest {
    pub input_asset: DepositAsset,
    pub output_asset: DepositAsset,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_decimals: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_decimals: Option<u8>,
    #[serde(flatten)]
    pub order_kind: MarketOrderKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender_address: Option<String>,
    pub recipient_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeQuote {
    pub provider_id: String,
    pub amount_in: String,
    pub amount_out: String,
    pub min_amount_out: Option<String>,
    pub max_amount_in: Option<String>,
    pub provider_quote: Value,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum ExchangeExecutionRequest {
    HyperliquidTrade(HyperliquidTradeStepRequest),
    HyperliquidLimitOrder(HyperliquidLimitOrderStepRequest),
    UniversalRouterSwap(UniversalRouterSwapStepRequest),
}

impl ExchangeExecutionRequest {
    pub fn hyperliquid_trade_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "hyperliquid trade step request").map(Self::HyperliquidTrade)
    }

    pub fn hyperliquid_limit_order_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "hyperliquid limit-order step request")
            .map(Self::HyperliquidLimitOrder)
    }

    pub fn universal_router_swap_from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "universal router swap step request")
            .map(Self::UniversalRouterSwap)
    }

    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::HyperliquidTrade(_) => "hyperliquid_trade",
            Self::HyperliquidLimitOrder(_) => "hyperliquid_limit_order",
            Self::UniversalRouterSwap(_) => "universal_router_swap",
        }
    }
}

fn decode_step_request<T>(value: &Value, label: &str) -> ProviderResult<T>
where
    T: DeserializeOwned,
{
    serde_json::from_value(value.clone()).map_err(|err| format!("invalid {label}: {err}"))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderExecutionState {
    #[serde(default)]
    pub operation: Option<ProviderOperationIntent>,
    #[serde(default)]
    pub addresses: Vec<ProviderAddressIntent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationIntent {
    pub operation_type: ProviderOperationType,
    pub status: ProviderOperationStatus,
    #[serde(default)]
    pub provider_ref: Option<String>,
    #[serde(default)]
    pub request: Option<Value>,
    #[serde(default)]
    pub response: Option<Value>,
    #[serde(default)]
    pub observed_state: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationObservationRequest {
    pub operation_id: Uuid,
    pub operation_type: ProviderOperationType,
    pub provider_ref: Option<String>,
    pub request: Value,
    pub response: Value,
    pub observed_state: Value,
    #[serde(default)]
    pub hint_evidence: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationObservation {
    pub status: ProviderOperationStatus,
    #[serde(default)]
    pub provider_ref: Option<String>,
    #[serde(default)]
    pub observed_state: Value,
    #[serde(default)]
    pub response: Option<Value>,
    #[serde(default)]
    pub tx_hash: Option<String>,
    #[serde(default)]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderAddressIntent {
    pub role: ProviderAddressRole,
    #[serde(rename = "chain_id")]
    pub chain: ChainId,
    #[serde(default, rename = "asset_id")]
    pub asset: Option<AssetId>,
    pub address: String,
    #[serde(default)]
    pub memo: Option<String>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderExecutionIntent {
    ProviderOnly {
        response: Value,
        #[serde(default)]
        state: ProviderExecutionState,
    },
    CustodyAction {
        custody_vault_id: Uuid,
        action: CustodyAction,
        #[serde(default)]
        provider_context: Value,
        #[serde(default)]
        state: ProviderExecutionState,
    },
    /// Sequential batch of custody actions issued against a single vault —
    /// e.g. an ERC-20 `approve` followed by an Across `deposit`. The executor
    /// submits each action in order, collecting logs, then invokes the
    /// originating provider's `post_execute` hook so it can derive observed
    /// state (deposit id, refund hash, …) from the on-chain events.
    CustodyActions {
        custody_vault_id: Uuid,
        actions: Vec<CustodyAction>,
        #[serde(default)]
        provider_context: Value,
        #[serde(default)]
        state: ProviderExecutionState,
    },
}

/// Patch returned from `BridgeProvider::post_execute` (and its peers).
/// Non-`None` fields overwrite the corresponding values on the
/// `ProviderOperationIntent` persisted by the executor.
#[derive(Debug, Default, Clone)]
pub struct ProviderExecutionStatePatch {
    pub provider_ref: Option<String>,
    pub observed_state: Option<Value>,
    pub response: Option<Value>,
    pub status: Option<ProviderOperationStatus>,
}

pub trait BridgeProvider: Send + Sync {
    fn id(&self) -> &str;

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>>;

    fn execute_bridge<'a>(
        &'a self,
        request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent>;

    /// Called by the executor after a `CustodyActions` intent has finished
    /// submitting every action on-chain. The provider inspects the receipts
    /// (and their event logs) to derive observed state — e.g. Across decodes
    /// the `FundsDeposited` event to record the deposit id for later polling.
    fn post_execute<'a>(
        &'a self,
        _provider_context: &'a Value,
        _receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async { Ok(ProviderExecutionStatePatch::default()) })
    }

    fn observe_bridge_operation<'a>(
        &'a self,
        _request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async { Ok(None) })
    }
}

pub trait UnitProvider: Send + Sync {
    fn id(&self) -> &str;

    fn supports_deposit(&self, asset: &DepositAsset) -> bool;

    fn supports_withdrawal(&self, asset: &DepositAsset) -> bool;

    fn execute_deposit<'a>(
        &'a self,
        request: &'a UnitDepositStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent>;

    fn execute_withdrawal<'a>(
        &'a self,
        request: &'a UnitWithdrawalStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent>;

    /// Called after a `CustodyActions` intent finishes. For HyperUnit this is
    /// the transition point from "our on-chain send committed" to "waiting for
    /// HyperUnit guardians to bridge it" — so the default flips the persisted
    /// operation to `WaitingExternal` without modifying observed state.
    fn post_execute<'a>(
        &'a self,
        _provider_context: &'a Value,
        _receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async {
            Ok(ProviderExecutionStatePatch {
                status: Some(ProviderOperationStatus::WaitingExternal),
                ..ProviderExecutionStatePatch::default()
            })
        })
    }

    fn observe_unit_operation<'a>(
        &'a self,
        _request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async { Ok(None) })
    }
}

pub trait ExchangeProvider: Send + Sync {
    fn id(&self) -> &str;

    fn quote_trade<'a>(
        &'a self,
        request: ExchangeQuoteRequest,
    ) -> ProviderFuture<'a, Option<ExchangeQuote>>;

    fn execute_trade<'a>(
        &'a self,
        request: &'a ExchangeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent>;

    /// Called after a `CustodyActions` intent finishes. Exchanges like HL
    /// return JSON order-ids rather than emitting logs, so the provider
    /// inspects `CustodyActionReceipt::response` to record `provider_ref`
    /// (the oid) and `observed_state`.
    fn post_execute<'a>(
        &'a self,
        _provider_context: &'a Value,
        _receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async { Ok(ProviderExecutionStatePatch::default()) })
    }

    fn observe_trade_operation<'a>(
        &'a self,
        _request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async { Ok(None) })
    }
}

/// Runtime registry of concrete provider adapters.
///
/// `AssetRegistry` answers "what transitions exist?" and this registry answers
/// "which concrete provider implementation can quote/execute/observe them at
/// runtime?" A venue is only usable when both registries are updated.
#[derive(Clone, Default)]
pub struct ActionProviderRegistry {
    asset_registry: Arc<AssetRegistry>,
    bridges: Vec<Arc<dyn BridgeProvider>>,
    units: Vec<Arc<dyn UnitProvider>>,
    exchanges: Vec<Arc<dyn ExchangeProvider>>,
}

#[derive(Debug, Clone)]
pub struct AcrossHttpProviderConfig {
    pub base_url: String,
    pub api_key: String,
    pub integrator_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CctpHttpProviderConfig {
    pub base_url: String,
    pub token_messenger_v2_address: String,
    pub message_transmitter_v2_address: String,
}

#[derive(Debug, Clone)]
pub struct VeloraHttpProviderConfig {
    pub base_url: String,
    pub partner: Option<String>,
}

impl VeloraHttpProviderConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            partner: Some(VELORA_DEFAULT_PARTNER.to_string()),
        }
    }

    #[must_use]
    pub fn with_partner(mut self, partner: Option<String>) -> Self {
        self.partner = partner;
        self
    }
}

impl AcrossHttpProviderConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let api_key = api_key.into().trim().to_string();
        assert!(!api_key.is_empty(), "Across API key must not be empty");
        Self {
            base_url: base_url.into(),
            api_key,
            integrator_id: None,
        }
    }

    #[must_use]
    pub fn with_integrator_id(mut self, integrator_id: Option<String>) -> Self {
        self.integrator_id = integrator_id;
        self
    }
}

impl CctpHttpProviderConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token_messenger_v2_address: CCTP_TOKEN_MESSENGER_V2_ADDRESS.to_string(),
            message_transmitter_v2_address: CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS.to_string(),
        }
    }

    #[must_use]
    pub fn mock(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token_messenger_v2_address: MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS.to_string(),
            message_transmitter_v2_address: MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS.to_string(),
        }
    }

    #[must_use]
    pub fn with_contract_addresses(
        mut self,
        token_messenger_v2_address: Option<String>,
        message_transmitter_v2_address: Option<String>,
    ) -> Self {
        if let Some(address) = token_messenger_v2_address {
            self.token_messenger_v2_address = address;
        }
        if let Some(address) = message_transmitter_v2_address {
            self.message_transmitter_v2_address = address;
        }
        self
    }
}

impl ActionProviderRegistry {
    #[must_use]
    pub fn new(
        bridges: Vec<Arc<dyn BridgeProvider>>,
        units: Vec<Arc<dyn UnitProvider>>,
        exchanges: Vec<Arc<dyn ExchangeProvider>>,
    ) -> Self {
        Self::with_asset_registry(
            Arc::new(AssetRegistry::default()),
            bridges,
            units,
            exchanges,
        )
    }

    #[must_use]
    pub fn with_asset_registry(
        asset_registry: Arc<AssetRegistry>,
        bridges: Vec<Arc<dyn BridgeProvider>>,
        units: Vec<Arc<dyn UnitProvider>>,
        exchanges: Vec<Arc<dyn ExchangeProvider>>,
    ) -> Self {
        Self {
            asset_registry,
            bridges,
            units,
            exchanges,
        }
    }

    #[must_use]
    pub fn mock_http(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        Self::http_with_options(
            Some(AcrossHttpProviderConfig::new(
                base_url.clone(),
                MOCK_ACROSS_API_KEY,
            )),
            Some(CctpHttpProviderConfig::mock(base_url.clone())),
            Some(base_url.clone()),
            None,
            Some(base_url),
            None,
            HyperliquidCallNetwork::Mainnet,
        )
        .expect("mock HTTP providers should always build")
    }

    pub fn http(
        across: Option<AcrossHttpProviderConfig>,
        cctp: Option<CctpHttpProviderConfig>,
        hyperunit_base_url: Option<String>,
        hyperunit_proxy_url: Option<String>,
        hyperliquid_base_url: Option<String>,
        velora: Option<VeloraHttpProviderConfig>,
        hyperliquid_network: HyperliquidCallNetwork,
    ) -> Result<Self, String> {
        Self::http_with_options(
            across,
            cctp,
            hyperunit_base_url,
            hyperunit_proxy_url,
            hyperliquid_base_url,
            velora,
            hyperliquid_network,
        )
    }

    pub fn http_with_options(
        across: Option<AcrossHttpProviderConfig>,
        cctp: Option<CctpHttpProviderConfig>,
        hyperunit_base_url: Option<String>,
        hyperunit_proxy_url: Option<String>,
        hyperliquid_base_url: Option<String>,
        velora: Option<VeloraHttpProviderConfig>,
        hyperliquid_network: HyperliquidCallNetwork,
    ) -> Result<Self, String> {
        Self::http_with_options_and_hyperliquid_timeout(
            across,
            cctp,
            hyperunit_base_url,
            hyperunit_proxy_url,
            hyperliquid_base_url,
            velora,
            hyperliquid_network,
            DEFAULT_HYPERLIQUID_ORDER_TIMEOUT_MS,
        )
    }

    pub fn http_with_options_and_hyperliquid_timeout(
        across: Option<AcrossHttpProviderConfig>,
        cctp: Option<CctpHttpProviderConfig>,
        hyperunit_base_url: Option<String>,
        hyperunit_proxy_url: Option<String>,
        hyperliquid_base_url: Option<String>,
        velora: Option<VeloraHttpProviderConfig>,
        hyperliquid_network: HyperliquidCallNetwork,
        hyperliquid_order_timeout_ms: u64,
    ) -> Result<Self, String> {
        let asset_registry = Arc::new(AssetRegistry::default());
        let mut bridges = Vec::<Arc<dyn BridgeProvider>>::new();
        if let Some(config) = across {
            let mut provider = AcrossProvider::new(config.base_url, config.api_key);
            if let Some(integrator_id) = config.integrator_id {
                provider = provider.with_integrator_id(integrator_id);
            }
            bridges.push(Arc::new(provider));
        }
        if let Some(config) = cctp {
            bridges.push(Arc::new(CctpProvider::new(
                config.base_url,
                config.token_messenger_v2_address,
                config.message_transmitter_v2_address,
                asset_registry.clone(),
            )?));
        }
        if let Some(base_url) = hyperliquid_base_url.clone() {
            bridges.push(Arc::new(HyperliquidBridgeProvider::new(
                base_url,
                hyperliquid_network,
                asset_registry.clone(),
            )));
        }
        let hyperliquid_target_base_url = hyperliquid_base_url.clone();
        let units = hyperunit_base_url
            .map(|base_url| {
                HyperUnitProvider::new(
                    base_url,
                    hyperunit_proxy_url.clone(),
                    asset_registry.clone(),
                    hyperliquid_target_base_url.clone().unwrap_or_default(),
                    hyperliquid_network,
                )
                .map(|provider| Arc::new(provider) as Arc<dyn UnitProvider>)
            })
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let mut exchanges: Vec<Arc<dyn ExchangeProvider>> = hyperliquid_base_url
            .map(|base_url| {
                Arc::new(HyperliquidProvider::new(
                    base_url,
                    hyperliquid_network,
                    asset_registry.clone(),
                    hyperliquid_order_timeout_ms,
                )) as Arc<dyn ExchangeProvider>
            })
            .into_iter()
            .collect();
        if let Some(config) = velora {
            exchanges.push(Arc::new(VeloraProvider::new(
                config.base_url,
                config.partner,
            )));
        }
        Ok(Self::with_asset_registry(
            asset_registry,
            bridges,
            units,
            exchanges,
        ))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bridges.is_empty() && self.units.is_empty() && self.exchanges.is_empty()
    }

    #[must_use]
    pub fn asset_registry(&self) -> Arc<AssetRegistry> {
        self.asset_registry.clone()
    }

    #[must_use]
    pub fn bridges(&self) -> &[Arc<dyn BridgeProvider>] {
        &self.bridges
    }

    #[must_use]
    pub fn units(&self) -> &[Arc<dyn UnitProvider>] {
        &self.units
    }

    #[must_use]
    pub fn exchanges(&self) -> &[Arc<dyn ExchangeProvider>] {
        &self.exchanges
    }

    pub fn bridge(&self, id: &str) -> Option<Arc<dyn BridgeProvider>> {
        self.bridges
            .iter()
            .find(|provider| provider.id() == id)
            .cloned()
    }

    pub fn unit(&self, id: &str) -> Option<Arc<dyn UnitProvider>> {
        self.units
            .iter()
            .find(|provider| provider.id() == id)
            .cloned()
    }

    pub fn exchange(&self, id: &str) -> Option<Arc<dyn ExchangeProvider>> {
        self.exchanges
            .iter()
            .find(|provider| provider.id() == id)
            .cloned()
    }
}

#[derive(Clone)]
pub struct AcrossProvider {
    client: AcrossClient,
    integrator_id: String,
}

impl AcrossProvider {
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            client: AcrossClient::new(normalize_base_url(base_url), api_key),
            integrator_id: ACROSS_INTEGRATOR_ID.to_string(),
        }
    }

    #[must_use]
    pub fn with_integrator_id(mut self, integrator_id: impl Into<String>) -> Self {
        self.integrator_id = integrator_id.into();
        self
    }
}

impl BridgeProvider for AcrossProvider {
    fn id(&self) -> &str {
        "across"
    }

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>> {
        Box::pin(async move {
            let Some(across_request) =
                build_across_swap_approval_request(&request, &self.integrator_id)
            else {
                return Ok(None);
            };
            let response = self
                .client
                .swap_approval(across_request)
                .await
                .map_err(|err| err.to_string())?;
            let Some(expected_output) = response.expected_output_amount.clone() else {
                return Ok(None);
            };
            let amount_in = match &request.order_kind {
                MarketOrderKind::ExactIn { amount_in, .. } => amount_in.clone(),
                MarketOrderKind::ExactOut { .. } => response
                    .max_input_amount
                    .clone()
                    .or_else(|| response.input_amount.clone())
                    .ok_or_else(|| {
                        "Across response missing inputAmount/maxInputAmount".to_string()
                    })?,
            };
            let expires_at = response
                .quote_expiry_timestamp
                .and_then(|ts| Utc.timestamp_opt(ts, 0).single())
                .unwrap_or_else(|| Utc::now() + chrono::Duration::seconds(30));
            let provider_quote = serde_json::to_value(&response).map_err(|err| err.to_string())?;
            Ok(Some(BridgeQuote {
                provider_id: self.id().to_string(),
                amount_in,
                amount_out: expected_output,
                provider_quote,
                expires_at,
            }))
        })
    }

    fn execute_bridge<'a>(
        &'a self,
        request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            let BridgeExecutionRequest::Across(step) = request else {
                return Err(format!(
                    "across cannot execute bridge request kind {}",
                    request.kind()
                ));
            };

            let origin_chain = ChainId::parse(&step.origin_chain_id)
                .map_err(|err| format!("invalid origin_chain_id: {err}"))?;
            let destination_chain = ChainId::parse(&step.destination_chain_id)
                .map_err(|err| format!("invalid destination_chain_id: {err}"))?;
            let origin_chain_id = origin_chain.evm_chain_id().ok_or_else(|| {
                format!("origin chain {} is not an evm chain", step.origin_chain_id)
            })?;
            let destination_chain_id = destination_chain.evm_chain_id().ok_or_else(|| {
                format!(
                    "destination chain {} is not an evm chain",
                    step.destination_chain_id
                )
            })?;

            let input_asset = AssetId::parse(&step.input_asset)
                .map_err(|err| format!("invalid input_asset: {err}"))?;
            let output_asset = AssetId::parse(&step.output_asset)
                .map_err(|err| format!("invalid output_asset: {err}"))?;
            let input_token = evm_token_address(&input_asset).ok_or_else(|| {
                format!("input asset {} is not a valid evm token", step.input_asset)
            })?;
            let output_token = evm_token_address(&output_asset).ok_or_else(|| {
                format!(
                    "output asset {} is not a valid evm token",
                    step.output_asset
                )
            })?;
            let depositor = Address::from_str(&step.depositor_address)
                .map_err(|err| format!("invalid depositor_address: {err}"))?;
            let refund_address = Address::from_str(&step.refund_address)
                .map_err(|err| format!("invalid refund_address: {err}"))?;
            let amount = U256::from_str_radix(&step.amount, 10)
                .map_err(|err| format!("invalid amount {}: {err}", step.amount))?;

            let across_request = AcrossSwapApprovalRequest {
                trade_type: "exactInput".to_string(),
                origin_chain_id,
                destination_chain_id,
                input_token,
                output_token,
                amount,
                depositor,
                recipient: step.recipient.clone(),
                refund_address,
                refund_on_origin: true,
                strict_trade_type: true,
                integrator_id: self.integrator_id.clone(),
                slippage: None,
                extra_query: BTreeMap::new(),
            };

            let response = self
                .client
                .swap_approval(across_request)
                .await
                .map_err(|err| err.to_string())?;
            let swap_tx = response
                .swap_tx
                .clone()
                .ok_or_else(|| "across /swap/approval response missing swapTx".to_string())?;

            let mut actions: Vec<CustodyAction> = Vec::new();
            if let Some(approvals) = &response.approval_txns {
                for approval in approvals {
                    actions.push(across_tx_to_action(approval)?);
                }
            }
            actions.push(across_tx_to_action(&swap_tx)?);

            let response_json = serde_json::to_value(&response).map_err(|err| err.to_string())?;
            let operation_request = json!({
                "origin_chain_id": origin_chain_id,
                "destination_chain_id": destination_chain_id,
            });
            let provider_context = json!({
                "origin_chain_id": origin_chain_id,
                "destination_chain_id": destination_chain_id,
            });

            Ok(ProviderExecutionIntent::CustodyActions {
                custody_vault_id: step.depositor_custody_vault_id,
                actions,
                provider_context,
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::AcrossBridge,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: None,
                        request: Some(operation_request),
                        response: Some(response_json),
                        observed_state: None,
                    }),
                    addresses: vec![],
                },
            })
        })
    }

    fn post_execute<'a>(
        &'a self,
        _provider_context: &'a Value,
        receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async move {
            // The deposit call is always the last action (approvals first, then deposit).
            let deposit_receipt = receipts
                .last()
                .ok_or_else(|| "across post_execute: no custody action receipts".to_string())?;

            let event = deposit_receipt
                .logs
                .iter()
                .find_map(|log| FundsDeposited::decode_log(&log.inner).ok())
                .ok_or_else(|| {
                    "across post_execute: deposit receipt missing FundsDeposited log".to_string()
                })?
                .data;

            let observed_state = json!({
                "deposit_id": event.depositId.to_string(),
                "destination_chain_id": event.destinationChainId.to_string(),
                "deposit_tx_hash": &deposit_receipt.tx_hash,
                "input_amount": event.inputAmount.to_string(),
                "output_amount": event.outputAmount.to_string(),
                "fill_deadline": event.fillDeadline,
                "quote_timestamp": event.quoteTimestamp,
            });

            Ok(ProviderExecutionStatePatch {
                provider_ref: Some(event.depositId.to_string()),
                observed_state: Some(observed_state),
                response: None,
                status: Some(ProviderOperationStatus::WaitingExternal),
            })
        })
    }

    fn observe_bridge_operation<'a>(
        &'a self,
        request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async move {
            let origin_chain_id = request
                .request
                .get("origin_chain_id")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    "across observe: operation request missing origin_chain_id".to_string()
                })?;
            let deposit_id_str = request.provider_ref.as_deref().ok_or_else(|| {
                "across observe: operation is missing provider_ref (deposit_id)".to_string()
            })?;
            let deposit_id = U256::from_str_radix(deposit_id_str, 10).map_err(|err| {
                format!("across observe: invalid deposit_id {deposit_id_str}: {err}")
            })?;

            let status = match self
                .client
                .deposit_status(AcrossDepositStatusRequest {
                    origin_chain_id,
                    deposit_id,
                })
                .await
            {
                Ok(status) => status,
                Err(AcrossClientError::HttpStatus { status: 404, body })
                    if body.contains("DepositNotFoundException") =>
                {
                    return Ok(Some(ProviderOperationObservation {
                        status: ProviderOperationStatus::WaitingExternal,
                        provider_ref: Some(deposit_id_str.to_string()),
                        observed_state: json!({
                            "status": "not_found",
                            "body": serde_json::from_str::<Value>(&body).unwrap_or(Value::String(body)),
                        }),
                        response: None,
                        tx_hash: None,
                        error: None,
                    }));
                }
                Err(err) => return Err(err.to_string()),
            };

            let mapped_status = map_deposit_status(&status.status);
            let tx_hash = status
                .fill_tx
                .clone()
                .or_else(|| status.deposit_refund_tx_hash.clone());
            let observed_state = serde_json::to_value(&status).map_err(|err| err.to_string())?;

            Ok(Some(ProviderOperationObservation {
                status: mapped_status,
                provider_ref: Some(deposit_id_str.to_string()),
                observed_state,
                response: None,
                tx_hash,
                error: None,
            }))
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AcrossExecuteStepRequest {
    pub origin_chain_id: String,
    pub destination_chain_id: String,
    pub input_asset: String,
    pub output_asset: String,
    pub amount: String,
    pub recipient: String,
    pub refund_address: String,
    pub depositor_address: String,
    pub depositor_custody_vault_id: Uuid,
}

fn across_tx_to_action(tx: &AcrossTransaction) -> ProviderResult<CustodyAction> {
    let value = parse_optional_u256("value", &tx.value)
        .map_err(|err| format!("invalid across tx value: {err}"))?
        .unwrap_or(U256::ZERO);
    Ok(CustodyAction::Call(ChainCall::Evm(EvmCall {
        to_address: tx.to.clone(),
        value: value.to_string(),
        calldata: tx.data.clone(),
    })))
}

fn map_deposit_status(raw: &str) -> ProviderOperationStatus {
    match raw {
        "filled" => ProviderOperationStatus::Completed,
        "refunded" => ProviderOperationStatus::Failed,
        "expired" => ProviderOperationStatus::Expired,
        _ => ProviderOperationStatus::WaitingExternal,
    }
}

fn build_across_swap_approval_request(
    request: &BridgeQuoteRequest,
    integrator_id: &str,
) -> Option<AcrossSwapApprovalRequest> {
    let origin_chain_id = request.source_asset.chain.evm_chain_id()?;
    let destination_chain_id = request.destination_asset.chain.evm_chain_id()?;
    let input_token = evm_token_address(&request.source_asset.asset)?;
    let output_token = evm_token_address(&request.destination_asset.asset)?;
    let depositor = Address::from_str(&request.depositor_address).ok()?;
    let refund_address = depositor;
    let (trade_type, amount) = match &request.order_kind {
        MarketOrderKind::ExactIn { amount_in, .. } => (
            "exactInput".to_string(),
            U256::from_str_radix(amount_in, 10).ok()?,
        ),
        MarketOrderKind::ExactOut { amount_out, .. } => (
            "exactOutput".to_string(),
            U256::from_str_radix(amount_out, 10).ok()?,
        ),
    };
    Some(AcrossSwapApprovalRequest {
        trade_type,
        origin_chain_id,
        destination_chain_id,
        input_token,
        output_token,
        amount,
        depositor,
        recipient: request.recipient_address.clone(),
        refund_address,
        refund_on_origin: true,
        strict_trade_type: true,
        integrator_id: integrator_id.to_string(),
        slippage: None,
        extra_query: BTreeMap::new(),
    })
}

fn evm_token_address(asset: &AssetId) -> Option<Address> {
    match asset {
        AssetId::Native => Some(Address::ZERO),
        AssetId::Reference(addr) => Address::from_str(addr).ok(),
    }
}

/// HyperUnit integration that uses the real `GET /gen` + `GET /operations`
/// REST shape. Deposits emit a source-chain `Transfer` custody action to the
/// guardian-returned `protocol_address`; withdrawals emit a Hyperliquid
/// `SpotSend` out of the router's HL spot vault to the protocol address.
/// The `provider_ref` persisted on the operation is the `protocol_address`
/// and also the polling key for `/operations`.
#[derive(Clone)]
pub struct HyperUnitProvider {
    client: HyperUnitClient,
    asset_registry: Arc<AssetRegistry>,
    hyperliquid_http: HyperliquidHttpClient,
    hyperliquid_base_url: String,
    hyperliquid_spot_meta: Arc<OnceCell<SpotMeta>>,
    hyperliquid_network: HyperliquidCallNetwork,
}

impl HyperUnitProvider {
    pub fn new(
        base_url: impl Into<String>,
        proxy_url: Option<String>,
        asset_registry: Arc<AssetRegistry>,
        hyperliquid_base_url: impl Into<String>,
        hyperliquid_network: HyperliquidCallNetwork,
    ) -> Result<Self, String> {
        let hyperliquid_base_url = normalize_base_url(hyperliquid_base_url);
        let hyperliquid_http =
            HyperliquidHttpClient::new(rustls_http_client(), hyperliquid_base_url.clone());
        let client = HyperUnitClient::new_with_proxy_url(base_url, proxy_url)
            .map_err(|err| format!("hyperunit client configuration failed: {err}"))?;
        Ok(Self {
            client,
            asset_registry,
            hyperliquid_http,
            hyperliquid_base_url,
            hyperliquid_spot_meta: Arc::new(OnceCell::new()),
            hyperliquid_network,
        })
    }

    fn resolve_unit_chain(&self, chain: &ChainId) -> ProviderResult<UnitChain> {
        let entry = self
            .asset_registry
            .provider_assets()
            .iter()
            .find(|entry| entry.provider == ProviderId::Unit && entry.chain == *chain)
            .ok_or_else(|| {
                format!("hyperunit: chain {chain} is not registered as a unit provider chain")
            })?;
        UnitChain::parse(&entry.provider_chain).ok_or_else(|| {
            format!(
                "hyperunit: wire chain {} (router chain {chain}) is not supported by hyperunit-client",
                entry.provider_chain
            )
        })
    }

    fn resolve_unit_asset(
        &self,
        asset: &DepositAsset,
        capability: ProviderAssetCapability,
    ) -> ProviderResult<UnitAsset> {
        let entry = self
            .asset_registry
            .provider_asset(ProviderId::Unit, asset, capability)
            .ok_or_else(|| {
                format!(
                    "hyperunit: asset {} on {} not registered for {capability:?}",
                    asset.asset, asset.chain
                )
            })?;
        UnitAsset::parse(&entry.provider_asset).ok_or_else(|| {
            format!(
                "hyperunit: asset {} not supported by hyperunit-client",
                entry.provider_asset
            )
        })
    }

    fn resolve_hl_spot_token(
        &self,
        asset: &DepositAsset,
        capability: ProviderAssetCapability,
    ) -> ProviderResult<(String, u8)> {
        let entry = self
            .asset_registry
            .provider_asset(ProviderId::Hyperliquid, asset, capability)
            .ok_or_else(|| {
                format!(
                    "hyperunit: asset {} on {} not registered as a hyperliquid provider asset for {capability:?}",
                    asset.asset, asset.chain
                )
            })?;
        Ok((entry.provider_asset.clone(), entry.decimals))
    }

    async fn hyperliquid_spot_meta(&self) -> ProviderResult<&SpotMeta> {
        self.hyperliquid_spot_meta
            .get_or_try_init(|| async {
                let req = json!({ "type": "spotMeta" });
                let meta: SpotMeta = self
                    .hyperliquid_http
                    .post_json("/info", &req)
                    .await
                    .map_err(|err| format!("hyperunit: fetch hyperliquid spotMeta: {err}"))?;
                Ok::<_, String>(meta)
            })
            .await
    }
}

impl UnitProvider for HyperUnitProvider {
    fn id(&self) -> &str {
        "unit"
    }

    fn supports_deposit(&self, asset: &DepositAsset) -> bool {
        self.asset_registry.supports_provider_capability(
            ProviderId::Unit,
            asset,
            ProviderAssetCapability::UnitDeposit,
        )
    }

    fn supports_withdrawal(&self, asset: &DepositAsset) -> bool {
        self.asset_registry.supports_provider_capability(
            ProviderId::Unit,
            asset,
            ProviderAssetCapability::UnitWithdrawal,
        )
    }

    fn execute_deposit<'a>(
        &'a self,
        step: &'a UnitDepositStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            let src_chain_id = ChainId::parse(&step.src_chain_id)
                .map_err(|err| format!("invalid src_chain_id: {err}"))?;
            let source_asset_id =
                AssetId::parse(&step.asset_id).map_err(|err| format!("invalid asset_id: {err}"))?;
            let source_asset = DepositAsset {
                chain: src_chain_id.clone(),
                asset: source_asset_id.clone(),
            };

            if step.dst_chain_id != "hyperliquid" {
                return Err(format!(
                    "hyperunit deposit destination must be hyperliquid, got {}",
                    step.dst_chain_id
                ));
            }

            let src_unit_chain = self.resolve_unit_chain(&src_chain_id)?;
            let unit_asset =
                self.resolve_unit_asset(&source_asset, ProviderAssetCapability::UnitDeposit)?;
            let dst_addr = step
                .hyperliquid_custody_vault_address
                .clone()
                .ok_or_else(|| {
                    "hyperunit deposit: hyperliquid_custody_vault_address must be hydrated"
                        .to_string()
                })?;
            let source_custody_vault_id = step.source_custody_vault_id.ok_or_else(|| {
                "hyperunit deposit: source_custody_vault_id is required".to_string()
            })?;

            let gen_response = self
                .client
                .generate_address(UnitGenerateAddressRequest {
                    src_chain: src_unit_chain,
                    dst_chain: UnitChain::Hyperliquid,
                    asset: unit_asset,
                    dst_addr: dst_addr.clone(),
                })
                .await
                .map_err(|err| format!("hyperunit /gen failed: {err}"))?;

            let protocol_address = gen_response.address.clone();
            let gen_response_json = serde_json::to_value(&gen_response)
                .map_err(|err| format!("serialize hyperunit /gen response: {err}"))?;

            let action = CustodyAction::Transfer {
                to_address: protocol_address.clone(),
                amount: step.amount.clone(),
            };

            let operation_request = json!({
                "protocol_address": protocol_address,
                "src_chain": src_unit_chain.as_wire_str(),
                "dst_chain": UnitChain::Hyperliquid.as_wire_str(),
                "asset": unit_asset.as_wire_str(),
                "dst_addr": dst_addr,
                "amount": step.amount,
            });

            Ok(ProviderExecutionIntent::CustodyActions {
                custody_vault_id: source_custody_vault_id,
                actions: vec![action],
                provider_context: operation_request.clone(),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::UnitDeposit,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: Some(protocol_address.clone()),
                        request: Some(operation_request),
                        response: Some(gen_response_json),
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
        step: &'a UnitWithdrawalStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            let dst_chain_id = ChainId::parse(&step.dst_chain_id)
                .map_err(|err| format!("invalid dst_chain_id: {err}"))?;
            let dest_asset_id =
                AssetId::parse(&step.asset_id).map_err(|err| format!("invalid asset_id: {err}"))?;
            let destination_asset = DepositAsset {
                chain: dst_chain_id.clone(),
                asset: dest_asset_id,
            };

            let dst_unit_chain = self.resolve_unit_chain(&dst_chain_id)?;
            let unit_asset = self
                .resolve_unit_asset(&destination_asset, ProviderAssetCapability::UnitWithdrawal)?;
            let (hl_symbol, hl_decimals) = self.resolve_hl_spot_token(
                &destination_asset,
                ProviderAssetCapability::ExchangeInput,
            )?;
            let hl_token = self
                .hyperliquid_spot_meta()
                .await?
                .spot_token_wire(&hl_symbol)
                .ok_or_else(|| {
                    format!(
                        "hyperunit: hyperliquid spot token {} is missing tokenId in spotMeta",
                        hl_symbol
                    )
                })?;

            let dst_addr = step.recipient_address.clone();
            let hyperliquid_custody_vault_id =
                step.hyperliquid_custody_vault_id.ok_or_else(|| {
                    "hyperunit withdrawal: hyperliquid_custody_vault_id must be hydrated"
                        .to_string()
                })?;
            let hyperliquid_custody_vault_address = step
                .hyperliquid_custody_vault_address
                .clone()
                .ok_or_else(|| {
                    "hyperunit withdrawal: hyperliquid_custody_vault_address must be hydrated"
                        .to_string()
                })?;
            let hyperliquid_user =
                Address::from_str(&hyperliquid_custody_vault_address).map_err(|err| {
                    format!(
                        "hyperunit withdrawal: invalid hyperliquid custody address {}: {err}",
                        hyperliquid_custody_vault_address
                    )
                })?;

            let gen_response = self
                .client
                .generate_address(UnitGenerateAddressRequest {
                    src_chain: UnitChain::Hyperliquid,
                    dst_chain: dst_unit_chain,
                    asset: unit_asset,
                    dst_addr: dst_addr.clone(),
                })
                .await
                .map_err(|err| format!("hyperunit /gen failed: {err}"))?;

            let protocol_address = gen_response.address.clone();
            let gen_response_json = serde_json::to_value(&gen_response)
                .map_err(|err| format!("serialize hyperunit /gen response: {err}"))?;
            let operations_before_by_protocol = self
                .client
                .operations(UnitOperationsRequest {
                    address: protocol_address.clone(),
                })
                .await
                .map_err(|err| format!("hyperunit /operations preflight failed: {err}"))?;
            let operations_before_by_destination = self
                .client
                .operations(UnitOperationsRequest {
                    address: dst_addr.clone(),
                })
                .await
                .map_err(|err| format!("hyperunit /operations preflight failed: {err}"))?;
            let seen_operation_fingerprints = observed_unit_operation_fingerprints(
                &operations_before_by_protocol.operations,
                &operations_before_by_destination.operations,
                &protocol_address,
            );

            let requested_amount_raw = U256::from_str_radix(&step.amount, 10).map_err(|err| {
                format!(
                    "hyperunit withdrawal: invalid requested amount {}: {err}",
                    step.amount
                )
            })?;
            let spot_state: SpotClearinghouseState = self
                .hyperliquid_http
                .post_json(
                    "/info",
                    &json!({
                        "type": "spotClearinghouseState",
                        "user": format!("{hyperliquid_user:#x}"),
                    }),
                )
                .await
                .map_err(|err| {
                    format!("hyperunit withdrawal: fetch hyperliquid spot state: {err}")
                })?;
            let available_amount_raw =
                parse_decimal_to_raw_units_floor(spot_state.balance_of(&hl_symbol), hl_decimals)?;
            let provider_minimum_raw = unit_withdrawal_minimum_raw(&destination_asset);
            let route_minimum_raw = step
                .min_amount_out
                .as_deref()
                .map(|raw| U256::from_str_radix(raw, 10))
                .transpose()
                .map_err(|err| format!("hyperunit withdrawal: invalid min_amount_out: {err}"))?
                .unwrap_or(U256::ZERO);
            let minimum_amount_raw = provider_minimum_raw.max(route_minimum_raw);
            let amount_raw = requested_amount_raw.min(available_amount_raw);
            if amount_raw < minimum_amount_raw {
                return Err(format!(
                    "hyperunit withdrawal: available Hyperliquid {hl_symbol} {} is below minimum {}",
                    available_amount_raw, minimum_amount_raw
                ));
            }
            if amount_raw.is_zero() {
                return Err(format!(
                    "hyperunit withdrawal: no available Hyperliquid {hl_symbol} balance"
                ));
            }
            let amount_decimal = format_hyperliquid_amount(&amount_raw.to_string(), hl_decimals)?;

            let action = CustodyAction::Call(ChainCall::Hyperliquid(HyperliquidCall {
                target_base_url: self.hyperliquid_base_url.clone(),
                network: self.hyperliquid_network,
                vault_address: None,
                payload: HyperliquidCallPayload::SpotSend {
                    destination: protocol_address.clone(),
                    token: hl_token,
                    amount: amount_decimal,
                },
            }));

            let operation_request = json!({
                "protocol_address": protocol_address,
                "src_chain": UnitChain::Hyperliquid.as_wire_str(),
                "dst_chain": dst_unit_chain.as_wire_str(),
                "asset": unit_asset.as_wire_str(),
                "dst_addr": dst_addr,
                "requested_amount": step.amount,
                "amount": amount_raw.to_string(),
                "spot_balance": spot_state,
                "spot_available_raw": available_amount_raw.to_string(),
                "minimum_amount_raw": minimum_amount_raw.to_string(),
                "seen_operation_fingerprints": seen_operation_fingerprints.iter().cloned().collect::<Vec<_>>(),
            });

            Ok(ProviderExecutionIntent::CustodyActions {
                custody_vault_id: hyperliquid_custody_vault_id,
                actions: vec![action],
                provider_context: operation_request.clone(),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::UnitWithdrawal,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: Some(protocol_address),
                        request: Some(operation_request),
                        response: Some(gen_response_json),
                        observed_state: None,
                    }),
                    addresses: vec![],
                },
            })
        })
    }

    fn observe_unit_operation<'a>(
        &'a self,
        request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async move {
            let protocol_address = request.provider_ref.as_deref().ok_or_else(|| {
                "hyperunit observe: operation is missing provider_ref (protocol_address)"
                    .to_string()
            })?;

            let response = self
                .client
                .operations(UnitOperationsRequest {
                    address: protocol_address.to_string(),
                })
                .await
                .map_err(|err| format!("hyperunit /operations failed: {err}"))?;

            let matched = response.operations.iter().find(|op| {
                op.matches_protocol_address(protocol_address)
                    && !op.has_seen_fingerprint(&seen_operation_fingerprints_from_request(
                        &request.request,
                    ))
            });

            let Some(op) = matched else {
                // Guardians haven't indexed a matching operation yet — still
                // waiting on the source-chain deposit to be finalized.
                let observed_state = serde_json::to_value(&response)
                    .map_err(|err| format!("serialize hyperunit /operations response: {err}"))?;
                return Ok(Some(ProviderOperationObservation {
                    status: ProviderOperationStatus::WaitingExternal,
                    provider_ref: Some(protocol_address.to_string()),
                    observed_state,
                    response: None,
                    tx_hash: None,
                    error: None,
                }));
            };

            let status = unit_operation_provider_status(op);
            let tx_hash = op
                .destination_tx_hash
                .clone()
                .or_else(|| op.source_tx_hash.clone());
            let observed_state = serde_json::to_value(op)
                .map_err(|err| format!("serialize hyperunit operation: {err}"))?;

            Ok(Some(ProviderOperationObservation {
                status,
                provider_ref: Some(protocol_address.to_string()),
                observed_state,
                response: None,
                tx_hash,
                error: None,
            }))
        })
    }
}

fn observed_unit_operation_fingerprints(
    protocol_operations: &[hyperunit_client::UnitOperation],
    destination_operations: &[hyperunit_client::UnitOperation],
    protocol_address: &str,
) -> BTreeSet<String> {
    protocol_operations
        .iter()
        .chain(destination_operations.iter())
        .filter(|operation| operation.matches_protocol_address(protocol_address))
        .flat_map(|operation| operation.fingerprints())
        .collect()
}

fn unit_operation_provider_status(op: &UnitOperation) -> ProviderOperationStatus {
    match op.classified_state() {
        UnitOperationState::Done => ProviderOperationStatus::Completed,
        UnitOperationState::Failure => ProviderOperationStatus::Failed,
        // HyperUnit can expose the destination transaction hash before it
        // advances the operation to `done`. At that point the destination-chain
        // transfer has been broadcast and the router has the terminal tx ref.
        UnitOperationState::QueuedForWithdraw if unit_operation_has_destination_tx(op) => {
            ProviderOperationStatus::Completed
        }
        _ => ProviderOperationStatus::WaitingExternal,
    }
}

fn unit_operation_has_destination_tx(op: &UnitOperation) -> bool {
    op.destination_tx_hash
        .as_deref()
        .is_some_and(|tx_hash| !tx_hash.trim().is_empty())
}

fn seen_operation_fingerprints_from_request(request: &Value) -> BTreeSet<String> {
    request
        .get("seen_operation_fingerprints")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

#[derive(Clone)]
pub struct HyperliquidBridgeProvider {
    network: HyperliquidCallNetwork,
    asset_registry: Arc<AssetRegistry>,
    http: HyperliquidHttpClient,
    target_base_url: String,
}

const HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW: u64 = 1_000_000;

impl HyperliquidBridgeProvider {
    #[must_use]
    pub fn new(
        base_url: impl Into<String>,
        network: HyperliquidCallNetwork,
        asset_registry: Arc<AssetRegistry>,
    ) -> Self {
        let target_base_url = normalize_base_url(base_url);
        let http = HyperliquidHttpClient::new(rustls_http_client(), target_base_url.clone());
        Self {
            network,
            asset_registry,
            http,
            target_base_url,
        }
    }

    fn resolve_bridge_input(&self, asset: &DepositAsset) -> ProviderResult<u8> {
        self.asset_registry
            .provider_asset(
                ProviderId::HyperliquidBridge,
                asset,
                ProviderAssetCapability::BridgeInput,
            )
            .map(|entry| entry.decimals)
            .ok_or_else(|| {
                format!(
                    "hyperliquid bridge does not register {} on {} as a bridge input",
                    asset.asset, asset.chain
                )
            })
    }

    fn resolve_bridge_output(&self, asset: &DepositAsset) -> ProviderResult<u8> {
        self.asset_registry
            .provider_asset(
                ProviderId::HyperliquidBridge,
                asset,
                ProviderAssetCapability::BridgeOutput,
            )
            .map(|entry| entry.decimals)
            .ok_or_else(|| {
                format!(
                    "hyperliquid bridge does not register {} on {} as a bridge output",
                    asset.asset, asset.chain
                )
            })
    }

    async fn clearinghouse_state(&self, user: &str) -> ProviderResult<ClearinghouseState> {
        let req = json!({
            "type": "clearinghouseState",
            "user": user,
        });
        self.http
            .post_json("/info", &req)
            .await
            .map_err(|err| format!("hyperliquid bridge /info clearinghouseState: {err}"))
    }

    async fn execute_withdrawal(
        &self,
        step: &HyperliquidBridgeWithdrawalStepRequest,
    ) -> ProviderResult<ProviderExecutionIntent> {
        let destination_chain = ChainId::parse(&step.destination_chain_id)
            .map_err(|err| format!("invalid destination_chain_id: {err}"))?;
        let output_asset_id = AssetId::parse(&step.output_asset)
            .map_err(|err| format!("invalid output_asset: {err}"))?;
        let destination_asset = DepositAsset {
            chain: destination_chain,
            asset: output_asset_id,
        };
        let decimals = self.resolve_bridge_output(&destination_asset)?;
        let source_custody_vault_id = step.hyperliquid_custody_vault_id.ok_or_else(|| {
            "hyperliquid bridge withdrawal: hyperliquid_custody_vault_id must be hydrated"
                .to_string()
        })?;
        let destination = Address::from_str(&step.recipient_address)
            .map_err(|err| format!("invalid hyperliquid withdrawal recipient: {err}"))?;
        let amount = format_hyperliquid_amount(&step.amount, decimals)?;
        let mut actions = Vec::new();
        if step.transfer_from_spot {
            actions.push(CustodyAction::Call(ChainCall::Hyperliquid(
                HyperliquidCall {
                    target_base_url: self.target_base_url.clone(),
                    network: self.network,
                    vault_address: None,
                    payload: HyperliquidCallPayload::UsdClassTransfer {
                        amount: amount.clone(),
                        to_perp: true,
                    },
                },
            )));
        }
        actions.push(CustodyAction::Call(ChainCall::Hyperliquid(
            HyperliquidCall {
                target_base_url: self.target_base_url.clone(),
                network: self.network,
                vault_address: None,
                payload: HyperliquidCallPayload::Withdraw3 {
                    destination: format!("{destination:#x}"),
                    amount: amount.clone(),
                },
            },
        )));
        let operation_request = json!({
            "destination_chain_id": step.destination_chain_id,
            "output_asset": step.output_asset,
            "amount": step.amount,
            "amount_decimal": amount,
            "recipient_address": format!("{destination:#x}"),
            "hyperliquid_custody_vault_id": source_custody_vault_id,
            "transfer_from_spot": step.transfer_from_spot,
            "withdraw_fee_raw": HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW.to_string(),
            "target_base_url": self.target_base_url,
            "network": self.network,
        });
        Ok(ProviderExecutionIntent::CustodyActions {
            custody_vault_id: source_custody_vault_id,
            actions,
            provider_context: json!({
                "kind": "hyperliquid_bridge_withdrawal",
            }),
            state: ProviderExecutionState {
                operation: Some(ProviderOperationIntent {
                    operation_type: ProviderOperationType::HyperliquidBridgeWithdrawal,
                    status: ProviderOperationStatus::Submitted,
                    provider_ref: None,
                    request: Some(operation_request),
                    response: None,
                    observed_state: None,
                }),
                addresses: vec![],
            },
        })
    }
}

impl BridgeProvider for HyperliquidBridgeProvider {
    fn id(&self) -> &str {
        "hyperliquid_bridge"
    }

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>> {
        Box::pin(async move {
            let is_deposit = request.destination_asset.chain.as_str() == "hyperliquid"
                && request.destination_asset.asset.is_native()
                && self
                    .asset_registry
                    .canonical_assets_match(&request.source_asset, &request.destination_asset)
                && self.resolve_bridge_input(&request.source_asset).is_ok();
            let is_withdrawal = request.source_asset.chain.as_str() == "hyperliquid"
                && request.source_asset.asset.is_native()
                && self
                    .asset_registry
                    .canonical_assets_match(&request.source_asset, &request.destination_asset)
                && self
                    .resolve_bridge_output(&request.destination_asset)
                    .is_ok();
            if !is_deposit && !is_withdrawal {
                return Ok(None);
            }

            let (amount_in, amount_out) = match &request.order_kind {
                MarketOrderKind::ExactIn { amount_in, .. } => {
                    let amount_raw = U256::from_str_radix(amount_in, 10)
                        .map_err(|err| format!("invalid amount: {err}"))?;
                    if is_withdrawal {
                        let fee = U256::from(HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW);
                        if amount_raw <= fee {
                            return Ok(None);
                        }
                        (
                            amount_in.clone(),
                            amount_raw.saturating_sub(fee).to_string(),
                        )
                    } else {
                        (amount_in.clone(), amount_in.clone())
                    }
                }
                MarketOrderKind::ExactOut { amount_out, .. } => {
                    let amount_raw = U256::from_str_radix(amount_out, 10)
                        .map_err(|err| format!("invalid amount: {err}"))?;
                    if is_withdrawal {
                        (
                            amount_raw
                                .saturating_add(U256::from(HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW))
                                .to_string(),
                            amount_out.clone(),
                        )
                    } else {
                        (amount_out.clone(), amount_out.clone())
                    }
                }
            };
            let amount_in_raw = U256::from_str_radix(&amount_in, 10)
                .map_err(|err| format!("invalid amount: {err}"))?;
            if is_deposit && amount_in_raw < minimum_bridge_deposit() {
                return Ok(None);
            }

            Ok(Some(BridgeQuote {
                provider_id: self.id().to_string(),
                amount_in,
                amount_out,
                provider_quote: json!({
                    "kind": if is_deposit { "hyperliquid_native_bridge" } else { "hyperliquid_bridge_withdrawal" },
                    "bridge_address": format!("{:#x}", hyperliquid_bridge_address(hyperliquid_api_network(self.network))),
                    "minimum_amount": minimum_bridge_deposit().to_string(),
                    "withdraw_fee_raw": if is_withdrawal { json!(HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW.to_string()) } else { Value::Null },
                }),
                expires_at: Utc::now() + chrono::Duration::seconds(30),
            }))
        })
    }

    fn execute_bridge<'a>(
        &'a self,
        request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            if let BridgeExecutionRequest::HyperliquidBridgeWithdrawal(step) = request {
                return self.execute_withdrawal(step).await;
            }
            let BridgeExecutionRequest::HyperliquidBridgeDeposit(step) = request else {
                return Err(format!(
                    "hyperliquid bridge cannot execute bridge request kind {}",
                    request.kind()
                ));
            };
            let source_chain = ChainId::parse(&step.source_chain_id)
                .map_err(|err| format!("invalid source_chain_id: {err}"))?;
            let input_asset_id = AssetId::parse(&step.input_asset)
                .map_err(|err| format!("invalid input_asset: {err}"))?;
            let source_asset = DepositAsset {
                chain: source_chain,
                asset: input_asset_id,
            };
            let decimals = self.resolve_bridge_input(&source_asset)?;
            let amount_raw = U256::from_str_radix(&step.amount, 10)
                .map_err(|err| format!("invalid amount {}: {err}", step.amount))?;
            if amount_raw < minimum_bridge_deposit() {
                return Err(format!(
                    "hyperliquid bridge deposit amount {} is below minimum {}",
                    step.amount,
                    minimum_bridge_deposit()
                ));
            }

            let source_custody_vault_id = step.source_custody_vault_id.ok_or_else(|| {
                "hyperliquid bridge: source_custody_vault_id must be hydrated".to_string()
            })?;
            let hyperliquid_user = step.source_custody_vault_address.clone().ok_or_else(|| {
                "hyperliquid bridge: source_custody_vault_address must be hydrated".to_string()
            })?;
            let before_state = self.clearinghouse_state(&hyperliquid_user).await?;
            let before_withdrawable_raw =
                parse_decimal_to_raw_units(&before_state.withdrawable, decimals)?;
            let expected_withdrawable_raw = before_withdrawable_raw + amount_raw;
            let bridge_address = format!(
                "{:#x}",
                hyperliquid_bridge_address(hyperliquid_api_network(self.network))
            );

            Ok(ProviderExecutionIntent::CustodyAction {
                custody_vault_id: source_custody_vault_id,
                action: CustodyAction::Transfer {
                    to_address: bridge_address.clone(),
                    amount: step.amount.clone(),
                },
                provider_context: json!({
                    "bridge_address": bridge_address,
                    "hyperliquid_user": hyperliquid_user,
                }),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::HyperliquidBridgeDeposit,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: Some(hyperliquid_user.clone()),
                        request: Some(json!({
                            "source_chain_id": step.source_chain_id,
                            "input_asset": step.input_asset,
                            "amount": step.amount,
                            "hyperliquid_user": hyperliquid_user,
                            "before_withdrawable_raw": before_withdrawable_raw.to_string(),
                            "expected_withdrawable_raw": expected_withdrawable_raw.to_string(),
                            "bridge_address": bridge_address,
                        })),
                        response: Some(json!({
                            "before_clearinghouse_state": before_state,
                        })),
                        observed_state: None,
                    }),
                    addresses: vec![],
                },
            })
        })
    }

    fn post_execute<'a>(
        &'a self,
        provider_context: &'a Value,
        receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async move {
            let receipt = receipts.last().ok_or_else(|| {
                "hyperliquid bridge post_execute: no custody action receipts".to_string()
            })?;
            if provider_context.get("kind").and_then(Value::as_str)
                == Some("hyperliquid_bridge_withdrawal")
            {
                return Ok(ProviderExecutionStatePatch {
                    provider_ref: Some(receipt.tx_hash.clone()),
                    observed_state: Some(json!({
                        "withdraw_tx_hash": receipt.tx_hash,
                    })),
                    status: Some(ProviderOperationStatus::Completed),
                    ..ProviderExecutionStatePatch::default()
                });
            }
            Ok(ProviderExecutionStatePatch {
                observed_state: Some(json!({
                    "deposit_tx_hash": receipt.tx_hash,
                })),
                status: Some(ProviderOperationStatus::WaitingExternal),
                ..ProviderExecutionStatePatch::default()
            })
        })
    }

    fn observe_bridge_operation<'a>(
        &'a self,
        request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async move {
            if request.operation_type == ProviderOperationType::HyperliquidBridgeWithdrawal {
                return Ok(Some(ProviderOperationObservation {
                    status: ProviderOperationStatus::Completed,
                    provider_ref: request.provider_ref.clone(),
                    observed_state: request.observed_state.clone(),
                    response: Some(request.response.clone()),
                    tx_hash: request.provider_ref.clone(),
                    error: None,
                }));
            }
            if request.operation_type != ProviderOperationType::HyperliquidBridgeDeposit {
                return Ok(None);
            }
            let user = request.provider_ref.as_deref().ok_or_else(|| {
                "hyperliquid bridge observe: provider_ref (hyperliquid user) is required"
                    .to_string()
            })?;
            let expected_withdrawable_raw = request
                .request
                .get("expected_withdrawable_raw")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    "hyperliquid bridge observe: request missing expected_withdrawable_raw"
                        .to_string()
                })
                .and_then(|raw| {
                    U256::from_str_radix(raw, 10).map_err(|err| {
                        format!(
                            "hyperliquid bridge observe: invalid expected_withdrawable_raw: {err}"
                        )
                    })
                })?;
            let state = self.clearinghouse_state(user).await?;
            let observed_withdrawable_raw = parse_decimal_to_raw_units(&state.withdrawable, 6)?;
            let status = if observed_withdrawable_raw >= expected_withdrawable_raw {
                ProviderOperationStatus::Completed
            } else {
                ProviderOperationStatus::WaitingExternal
            };

            Ok(Some(ProviderOperationObservation {
                status,
                provider_ref: Some(user.to_string()),
                observed_state: json!({
                    "clearinghouse_state": state,
                    "observed_withdrawable_raw": observed_withdrawable_raw.to_string(),
                    "expected_withdrawable_raw": expected_withdrawable_raw.to_string(),
                }),
                response: None,
                tx_hash: request
                    .observed_state
                    .get("deposit_tx_hash")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                error: None,
            }))
        })
    }
}

#[derive(Clone)]
pub struct CctpProvider {
    base_url: String,
    http: reqwest::Client,
    asset_registry: Arc<AssetRegistry>,
    token_messenger_v2_address: Address,
    message_transmitter_v2_address: Address,
}

impl CctpProvider {
    pub fn new(
        base_url: impl Into<String>,
        token_messenger_v2_address: impl AsRef<str>,
        message_transmitter_v2_address: impl AsRef<str>,
        asset_registry: Arc<AssetRegistry>,
    ) -> ProviderResult<Self> {
        let token_messenger_v2_address = Address::from_str(token_messenger_v2_address.as_ref())
            .map_err(|err| format!("invalid CCTP TokenMessengerV2 address: {err}"))?;
        let message_transmitter_v2_address =
            Address::from_str(message_transmitter_v2_address.as_ref())
                .map_err(|err| format!("invalid CCTP MessageTransmitterV2 address: {err}"))?;
        Ok(Self {
            base_url: normalize_base_url(base_url),
            http: rustls_http_client(),
            asset_registry,
            token_messenger_v2_address,
            message_transmitter_v2_address,
        })
    }

    fn resolve_bridge_assets(
        &self,
        source: &DepositAsset,
        destination: &DepositAsset,
    ) -> Option<CctpBridgeAssets> {
        if source == destination {
            return None;
        }
        if source.chain.evm_chain_id().is_none() || destination.chain.evm_chain_id().is_none() {
            return None;
        }
        if !self
            .asset_registry
            .canonical_assets_match(source, destination)
        {
            return None;
        }
        let input = self.asset_registry.provider_asset(
            ProviderId::Cctp,
            source,
            ProviderAssetCapability::BridgeInput,
        )?;
        let output = self.asset_registry.provider_asset(
            ProviderId::Cctp,
            destination,
            ProviderAssetCapability::BridgeOutput,
        )?;
        let source_domain = input.provider_chain.parse::<u32>().ok()?;
        let destination_domain = output.provider_chain.parse::<u32>().ok()?;
        let burn_token = Address::from_str(&input.provider_asset).ok()?;
        let destination_token = Address::from_str(&output.provider_asset).ok()?;
        Some(CctpBridgeAssets {
            source_domain,
            destination_domain,
            burn_token,
            destination_token,
        })
    }

    async fn fetch_messages(
        &self,
        source_domain: u32,
        tx_hash: &str,
    ) -> ProviderResult<Option<CctpMessagesResponse>> {
        let url = format!("{}/v2/messages/{source_domain}", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[("transactionHash", tx_hash)])
            .send()
            .await
            .map_err(|err| format!("cctp messages request failed: {err}"))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| format!("cctp messages response body failed: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "cctp messages request failed with {status}: {body}"
            ));
        }
        serde_json::from_str(&body)
            .map(Some)
            .map_err(|err| format!("cctp messages response was not JSON: {err}; body={body}"))
    }

    async fn fetch_burn_fees(
        &self,
        source_domain: u32,
        destination_domain: u32,
    ) -> ProviderResult<Vec<CctpBurnFeeEntry>> {
        let url = format!(
            "{}/v2/burn/USDC/fees/{source_domain}/{destination_domain}",
            self.base_url
        );
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|err| format!("cctp burn fees request failed: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| format!("cctp burn fees response body failed: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "cctp burn fees request failed with {status}: {body}"
            ));
        }
        serde_json::from_str(&body)
            .map_err(|err| format!("cctp burn fees response was not JSON: {err}; body={body}"))
    }

    async fn standard_burn_fee_bps_micros(
        &self,
        source_domain: u32,
        destination_domain: u32,
    ) -> ProviderResult<u128> {
        let fees = self
            .fetch_burn_fees(source_domain, destination_domain)
            .await?;
        let entry = fees
            .iter()
            .find(|fee| fee.finality_threshold == CCTP_STANDARD_FINALITY_THRESHOLD)
            .ok_or_else(|| {
                format!(
                    "cctp burn fees missing standard finality threshold {} for {source_domain}->{destination_domain}",
                    CCTP_STANDARD_FINALITY_THRESHOLD
                )
            })?;
        parse_decimal_bps_to_micros(&entry.minimum_fee_bps)
    }
}

impl BridgeProvider for CctpProvider {
    fn id(&self) -> &str {
        "cctp"
    }

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>> {
        Box::pin(async move {
            let Some(assets) =
                self.resolve_bridge_assets(&request.source_asset, &request.destination_asset)
            else {
                return Ok(None);
            };
            let fee_bps_micros = self
                .standard_burn_fee_bps_micros(assets.source_domain, assets.destination_domain)
                .await?;
            let Some(amounts) = cctp_quote_amounts(&request.order_kind, fee_bps_micros)? else {
                return Ok(None);
            };

            Ok(Some(BridgeQuote {
                provider_id: self.id().to_string(),
                amount_in: amounts.amount_in.to_string(),
                amount_out: amounts.amount_out.to_string(),
                provider_quote: json!({
                    "schema_version": 1,
                    "kind": "cctp_bridge",
                    "source_domain": assets.source_domain,
                    "destination_domain": assets.destination_domain,
                    "burn_token": format!("{:#x}", assets.burn_token),
                    "destination_token": format!("{:#x}", assets.destination_token),
                    "token_messenger_v2": format!("{:#x}", self.token_messenger_v2_address),
                    "message_transmitter_v2": format!("{:#x}", self.message_transmitter_v2_address),
                    "fee_bps": decimal_bps_string(fee_bps_micros),
                    "max_fee": amounts.max_fee.to_string(),
                    "min_finality_threshold": CCTP_STANDARD_FINALITY_THRESHOLD,
                    "transfer_mode": "standard",
                }),
                expires_at: Utc::now() + chrono::Duration::minutes(5),
            }))
        })
    }

    fn execute_bridge<'a>(
        &'a self,
        request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            match request {
                BridgeExecutionRequest::CctpBurn(step) => self.execute_burn(step).await,
                BridgeExecutionRequest::CctpReceive(step) => self.execute_receive(step).await,
                _ => Err(format!(
                    "cctp cannot execute bridge request kind {}",
                    request.kind()
                )),
            }
        })
    }

    fn post_execute<'a>(
        &'a self,
        _provider_context: &'a Value,
        receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async move {
            let burn_receipt = receipts
                .last()
                .ok_or_else(|| "cctp post_execute: no custody action receipts".to_string())?;
            Ok(ProviderExecutionStatePatch {
                provider_ref: Some(burn_receipt.tx_hash.clone()),
                observed_state: Some(json!({
                    "burn_tx_hash": burn_receipt.tx_hash,
                    "submitted_at": Utc::now(),
                })),
                response: None,
                status: Some(ProviderOperationStatus::WaitingExternal),
            })
        })
    }

    fn observe_bridge_operation<'a>(
        &'a self,
        request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async move {
            if request.operation_type != ProviderOperationType::CctpBridge {
                return Ok(None);
            }
            let source_domain = request
                .request
                .get("source_domain")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| "cctp observe: request missing source_domain".to_string())?;
            let burn_tx_hash = request
                .provider_ref
                .as_deref()
                .or_else(|| {
                    request
                        .observed_state
                        .get("burn_tx_hash")
                        .and_then(Value::as_str)
                })
                .ok_or_else(|| "cctp observe: operation missing burn tx hash".to_string())?;

            let Some(messages_response) = self.fetch_messages(source_domain, burn_tx_hash).await?
            else {
                return Ok(Some(ProviderOperationObservation {
                    status: ProviderOperationStatus::WaitingExternal,
                    provider_ref: Some(burn_tx_hash.to_string()),
                    observed_state: json!({
                        "burn_tx_hash": burn_tx_hash,
                        "attestation_status": "not_observed",
                    }),
                    response: None,
                    tx_hash: Some(burn_tx_hash.to_string()),
                    error: None,
                }));
            };
            let complete = messages_response.messages.iter().find(|message| {
                message.status.eq_ignore_ascii_case("complete")
                    && message
                        .message
                        .as_deref()
                        .is_some_and(|value| !value.is_empty())
                    && message
                        .attestation
                        .as_deref()
                        .is_some_and(|value| !value.is_empty())
            });
            if let Some(message) = complete {
                return Ok(Some(ProviderOperationObservation {
                    status: ProviderOperationStatus::Completed,
                    provider_ref: Some(burn_tx_hash.to_string()),
                    observed_state: json!({
                        "burn_tx_hash": burn_tx_hash,
                        "attestation_status": message.status,
                        "message": message.message,
                        "attestation": message.attestation,
                        "event_nonce": message.event_nonce,
                        "decoded_message_body": message.decoded_message_body,
                        "cctp_version": message.cctp_version,
                    }),
                    response: Some(serde_json::to_value(&messages_response).map_err(|err| {
                        format!("cctp observe: serialize messages response: {err}")
                    })?),
                    tx_hash: Some(burn_tx_hash.to_string()),
                    error: None,
                }));
            }

            Ok(Some(ProviderOperationObservation {
                status: ProviderOperationStatus::WaitingExternal,
                provider_ref: Some(burn_tx_hash.to_string()),
                observed_state: json!({
                    "burn_tx_hash": burn_tx_hash,
                    "attestation_status": "pending",
                    "messages": messages_response.messages,
                }),
                response: Some(
                    serde_json::to_value(&messages_response).map_err(|err| {
                        format!("cctp observe: serialize messages response: {err}")
                    })?,
                ),
                tx_hash: Some(burn_tx_hash.to_string()),
                error: None,
            }))
        })
    }
}

impl CctpProvider {
    async fn execute_burn(
        &self,
        step: &CctpBurnStepRequest,
    ) -> ProviderResult<ProviderExecutionIntent> {
        let source_asset = DepositAsset {
            chain: ChainId::parse(&step.source_chain_id)
                .map_err(|err| format!("invalid cctp source_chain_id: {err}"))?,
            asset: AssetId::parse(&step.input_asset)
                .map_err(|err| format!("invalid cctp input_asset: {err}"))?,
        };
        let destination_asset = DepositAsset {
            chain: ChainId::parse(&step.destination_chain_id)
                .map_err(|err| format!("invalid cctp destination_chain_id: {err}"))?,
            asset: AssetId::parse(&step.output_asset)
                .map_err(|err| format!("invalid cctp output_asset: {err}"))?,
        };
        let assets = self
            .resolve_bridge_assets(&source_asset, &destination_asset)
            .ok_or_else(|| "cctp bridge assets are not registered".to_string())?;
        let source_custody_vault_id = step
            .source_custody_vault_id
            .ok_or_else(|| "cctp burn: source_custody_vault_id must be hydrated".to_string())?;
        let mint_recipient = Address::from_str(&step.recipient_address)
            .map_err(|err| format!("invalid cctp recipient_address: {err}"))?;
        let amount = U256::from_str_radix(&step.amount, 10)
            .map_err(|err| format!("invalid cctp amount {}: {err}", step.amount))?;
        let max_fee_raw = step.max_fee.as_deref().unwrap_or("0");
        let max_fee = U256::from_str_radix(max_fee_raw, 10)
            .map_err(|err| format!("invalid cctp max_fee {max_fee_raw}: {err}"))?;

        let approve = CustodyAction::Call(ChainCall::Evm(EvmCall {
            to_address: format!("{:#x}", assets.burn_token),
            value: "0".to_string(),
            calldata: encode_erc20_approve(
                &format!("{:#x}", self.token_messenger_v2_address),
                &step.amount,
            )?,
        }));
        let call = ICircleTokenMessengerV2::depositForBurnCall {
            amount,
            destinationDomain: assets.destination_domain,
            mintRecipient: evm_address_to_bytes32(mint_recipient),
            burnToken: assets.burn_token,
            destinationCaller: FixedBytes::ZERO,
            maxFee: max_fee,
            minFinalityThreshold: CCTP_STANDARD_FINALITY_THRESHOLD,
        };
        let burn = CustodyAction::Call(ChainCall::Evm(EvmCall {
            to_address: format!("{:#x}", self.token_messenger_v2_address),
            value: "0".to_string(),
            calldata: format!("0x{}", hex::encode(call.abi_encode())),
        }));
        let operation_request = json!({
            "transition_decl_id": step.transition_decl_id,
            "source_chain_id": step.source_chain_id,
            "destination_chain_id": step.destination_chain_id,
            "input_asset": step.input_asset,
            "output_asset": step.output_asset,
            "amount": step.amount,
            "source_domain": assets.source_domain,
            "destination_domain": assets.destination_domain,
            "mint_recipient_address": step.recipient_address,
            "burn_token": format!("{:#x}", assets.burn_token),
            "destination_token": format!("{:#x}", assets.destination_token),
            "token_messenger_v2": format!("{:#x}", self.token_messenger_v2_address),
            "message_transmitter_v2": format!("{:#x}", self.message_transmitter_v2_address),
            "max_fee": max_fee.to_string(),
            "min_finality_threshold": CCTP_STANDARD_FINALITY_THRESHOLD,
        });

        Ok(ProviderExecutionIntent::CustodyActions {
            custody_vault_id: source_custody_vault_id,
            actions: vec![approve, burn],
            provider_context: operation_request.clone(),
            state: ProviderExecutionState {
                operation: Some(ProviderOperationIntent {
                    operation_type: ProviderOperationType::CctpBridge,
                    status: ProviderOperationStatus::Submitted,
                    provider_ref: None,
                    request: Some(operation_request),
                    response: Some(json!({
                        "kind": "cctp_burn_submitted",
                    })),
                    observed_state: None,
                }),
                addresses: vec![],
            },
        })
    }

    async fn execute_receive(
        &self,
        step: &CctpReceiveStepRequest,
    ) -> ProviderResult<ProviderExecutionIntent> {
        let source_custody_vault_id = step
            .source_custody_vault_id
            .ok_or_else(|| "cctp receive: source_custody_vault_id must be hydrated".to_string())?;
        let message = decode_hex_bytes("cctp message", &step.message)?;
        let attestation = decode_hex_bytes("cctp attestation", &step.attestation)?;
        let call = ICircleMessageTransmitterV2::receiveMessageCall {
            message: Bytes::from(message),
            attestation: Bytes::from(attestation),
        };
        Ok(ProviderExecutionIntent::CustodyAction {
            custody_vault_id: source_custody_vault_id,
            action: CustodyAction::Call(ChainCall::Evm(EvmCall {
                to_address: format!("{:#x}", self.message_transmitter_v2_address),
                value: "0".to_string(),
                calldata: format!("0x{}", hex::encode(call.abi_encode())),
            })),
            provider_context: json!({
                "kind": "cctp_receive",
                "burn_transition_decl_id": step.burn_transition_decl_id,
                "message_transmitter_v2": format!("{:#x}", self.message_transmitter_v2_address),
            }),
            state: ProviderExecutionState::default(),
        })
    }
}

#[derive(Debug, Clone)]
struct CctpBridgeAssets {
    source_domain: u32,
    destination_domain: u32,
    burn_token: Address,
    destination_token: Address,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CctpMessagesResponse {
    #[serde(default)]
    messages: Vec<CctpMessageEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct CctpBurnFeeEntry {
    #[serde(rename = "finalityThreshold")]
    finality_threshold: u32,
    #[serde(rename = "minimumFee", deserialize_with = "deserialize_decimal_string")]
    minimum_fee_bps: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct CctpMessageEntry {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    attestation: Option<String>,
    #[serde(default)]
    event_nonce: Option<String>,
    #[serde(default)]
    cctp_version: Option<u64>,
    #[serde(default)]
    status: String,
    #[serde(default)]
    decoded_message_body: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CctpQuoteAmounts {
    amount_in: U256,
    amount_out: U256,
    max_fee: U256,
}

fn cctp_quote_amounts(
    order_kind: &MarketOrderKind,
    fee_bps_micros: u128,
) -> ProviderResult<Option<CctpQuoteAmounts>> {
    let denominator = U256::from(10_000_000_000_u128);
    let fee = U256::from(fee_bps_micros);
    if fee >= denominator {
        return Err("cctp fee must be below 10000 bps".to_string());
    }
    match order_kind {
        MarketOrderKind::ExactIn { amount_in, .. } => {
            let amount_in = U256::from_str_radix(amount_in, 10)
                .map_err(|err| format!("invalid cctp amount_in {amount_in}: {err}"))?;
            if amount_in.is_zero() {
                return Ok(None);
            }
            let max_fee = cctp_fee_amount(amount_in, fee_bps_micros);
            if max_fee >= amount_in {
                return Ok(None);
            }
            Ok(Some(CctpQuoteAmounts {
                amount_in,
                amount_out: amount_in.saturating_sub(max_fee),
                max_fee,
            }))
        }
        MarketOrderKind::ExactOut { amount_out, .. } => {
            let amount_out = U256::from_str_radix(amount_out, 10)
                .map_err(|err| format!("invalid cctp amount_out {amount_out}: {err}"))?;
            if amount_out.is_zero() {
                return Ok(None);
            }
            let gross_denominator = denominator.saturating_sub(fee);
            let amount_in =
                div_ceil_u256(amount_out.saturating_mul(denominator), gross_denominator);
            let max_fee = amount_in.saturating_sub(amount_out);
            Ok(Some(CctpQuoteAmounts {
                amount_in,
                amount_out,
                max_fee,
            }))
        }
    }
}

fn cctp_fee_amount(amount: U256, fee_bps_micros: u128) -> U256 {
    div_ceil_u256(
        amount.saturating_mul(U256::from(fee_bps_micros)),
        U256::from(10_000_000_000_u128),
    )
}

fn div_ceil_u256(numerator: U256, denominator: U256) -> U256 {
    if numerator.is_zero() {
        return U256::ZERO;
    }
    numerator.saturating_add(denominator.saturating_sub(U256::from(1_u64))) / denominator
}

fn parse_decimal_bps_to_micros(raw: &str) -> ProviderResult<u128> {
    let value = raw.trim();
    if value.is_empty() || value.starts_with('-') {
        return Err(format!("invalid CCTP fee bps {raw:?}"));
    }
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty() || !whole.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!("invalid CCTP fee bps {raw:?}"));
    }
    if !fractional.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!("invalid CCTP fee bps {raw:?}"));
    }
    let whole = whole
        .parse::<u128>()
        .map_err(|err| format!("invalid CCTP fee bps {raw:?}: {err}"))?;
    let mut padded = fractional.chars().take(6).collect::<String>();
    while padded.len() < 6 {
        padded.push('0');
    }
    let mut fractional_micros = padded
        .parse::<u128>()
        .map_err(|err| format!("invalid CCTP fee bps {raw:?}: {err}"))?;
    if fractional.chars().skip(6).any(|ch| ch != '0') {
        fractional_micros = fractional_micros.saturating_add(1);
    }
    Ok(whole
        .saturating_mul(1_000_000)
        .saturating_add(fractional_micros))
}

fn decimal_bps_string(fee_bps_micros: u128) -> String {
    let whole = fee_bps_micros / 1_000_000;
    let fractional = fee_bps_micros % 1_000_000;
    if fractional == 0 {
        return whole.to_string();
    }
    let trimmed = format!("{fractional:06}").trim_end_matches('0').to_string();
    format!("{whole}.{trimmed}")
}

fn deserialize_decimal_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(value) => Ok(value),
        other => Err(serde::de::Error::custom(format!(
            "expected decimal string or number, got {other}"
        ))),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CctpBurnStepRequest {
    pub order_id: Uuid,
    pub transition_decl_id: String,
    pub source_chain_id: String,
    pub destination_chain_id: String,
    pub input_asset: String,
    pub output_asset: String,
    pub amount: String,
    pub recipient_address: String,
    #[serde(default)]
    pub recipient_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub recipient_custody_vault_role: Option<String>,
    #[serde(default)]
    pub source_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub source_custody_vault_address: Option<String>,
    #[serde(default)]
    pub max_fee: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CctpReceiveStepRequest {
    pub order_id: Uuid,
    pub burn_transition_decl_id: String,
    pub destination_chain_id: String,
    pub output_asset: String,
    pub amount: String,
    #[serde(default)]
    pub source_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub source_custody_vault_address: Option<String>,
    #[serde(default)]
    pub recipient_address: Option<String>,
    #[serde(default)]
    pub recipient_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub attestation: String,
}

fn evm_address_to_bytes32(address: Address) -> FixedBytes<32> {
    let mut bytes = [0_u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    FixedBytes::from(bytes)
}

fn decode_hex_bytes(field: &'static str, value: &str) -> ProviderResult<Vec<u8>> {
    let trimmed = value.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if hex.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    hex::decode(hex).map_err(|err| format!("invalid {field} hex: {err}"))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnitDepositStepRequest {
    pub order_id: Uuid,
    pub src_chain_id: String,
    pub dst_chain_id: String,
    pub asset_id: String,
    pub amount: String,
    pub source_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub hyperliquid_custody_vault_address: Option<String>,
}

impl UnitDepositStepRequest {
    pub fn from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "unit deposit step request")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnitWithdrawalStepRequest {
    pub order_id: Uuid,
    pub dst_chain_id: String,
    pub asset_id: String,
    pub amount: String,
    #[serde(default)]
    pub min_amount_out: Option<String>,
    pub recipient_address: String,
    #[serde(default)]
    pub hyperliquid_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub hyperliquid_custody_vault_address: Option<String>,
}

impl UnitWithdrawalStepRequest {
    pub fn from_value(value: &Value) -> ProviderResult<Self> {
        decode_step_request(value, "unit withdrawal step request")
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyperliquidBridgeDepositStepRequest {
    pub order_id: Uuid,
    pub source_chain_id: String,
    pub input_asset: String,
    pub amount: String,
    #[serde(default)]
    pub source_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub source_custody_vault_address: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyperliquidBridgeWithdrawalStepRequest {
    pub order_id: Uuid,
    pub destination_chain_id: String,
    pub output_asset: String,
    pub amount: String,
    pub recipient_address: String,
    #[serde(default)]
    pub transfer_from_spot: bool,
    #[serde(default)]
    pub hyperliquid_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub hyperliquid_custody_vault_address: Option<String>,
}

/// Convert a base-unit amount string (satoshis / wei / …) into the decimal
/// string Hyperliquid expects for user-action payloads. The HL SDK spec is
/// pinned in `hyperliquid-client::signature` — amount is `"1"`, `"0.5"`,
/// … in the asset's natural unit.
fn format_hyperliquid_amount(raw_amount: &str, decimals: u8) -> ProviderResult<String> {
    let digits = raw_amount.trim();
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!(
            "hyperliquid amount must be a non-negative integer string, got {raw_amount:?}"
        ));
    }
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return Ok(digits.trim_start_matches('0').to_string().or_zero());
    }
    let padded = if digits.len() <= decimals {
        format!("{:0>width$}", digits, width = decimals + 1)
    } else {
        digits.to_string()
    };
    let split_at = padded.len() - decimals;
    let (whole, frac) = padded.split_at(split_at);
    let whole_trimmed = whole.trim_start_matches('0');
    let whole_canonical = if whole_trimmed.is_empty() {
        "0"
    } else {
        whole_trimmed
    };
    let frac_trimmed = frac.trim_end_matches('0');
    if frac_trimmed.is_empty() {
        Ok(whole_canonical.to_string())
    } else {
        Ok(format!("{whole_canonical}.{frac_trimmed}"))
    }
}

fn parse_decimal_to_raw_units(value: &str, decimals: u8) -> ProviderResult<U256> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("decimal amount must not be empty".to_string());
    }
    let mut parts = trimmed.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err(format!(
            "invalid decimal amount {value:?}: multiple decimal points"
        ));
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit()) || !frac.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!(
            "invalid decimal amount {value:?}: expected only decimal digits"
        ));
    }
    let decimals = usize::from(decimals);
    if frac.len() > decimals {
        return Err(format!(
            "invalid decimal amount {value:?}: more than {decimals} fractional digits"
        ));
    }
    let combined = format!("{whole}{:0<width$}", frac, width = decimals);
    let normalized = combined.trim_start_matches('0');
    let digits = if normalized.is_empty() {
        "0"
    } else {
        normalized
    };
    U256::from_str_radix(digits, 10)
        .map_err(|err| format!("invalid decimal amount {value:?}: {err}"))
}

fn parse_decimal_to_raw_units_floor(value: &str, decimals: u8) -> ProviderResult<U256> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("decimal amount must not be empty".to_string());
    }
    let mut parts = trimmed.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err(format!(
            "invalid decimal amount {value:?}: multiple decimal points"
        ));
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit()) || !frac.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!(
            "invalid decimal amount {value:?}: expected only decimal digits"
        ));
    }
    let decimals = usize::from(decimals);
    let frac = if frac.len() > decimals {
        &frac[..decimals]
    } else {
        frac
    };
    let combined = format!("{whole}{:0<width$}", frac, width = decimals);
    let normalized = combined.trim_start_matches('0');
    let digits = if normalized.is_empty() {
        "0"
    } else {
        normalized
    };
    U256::from_str_radix(digits, 10)
        .map_err(|err| format!("invalid decimal amount {value:?}: {err}"))
}

fn unit_withdrawal_minimum_raw(asset: &DepositAsset) -> U256 {
    if asset.chain.as_str() == "bitcoin" && matches!(asset.asset, AssetId::Native) {
        U256::from(30_000u64)
    } else {
        U256::ZERO
    }
}

trait EmptyStringFallback {
    fn or_zero(self) -> String;
}

impl EmptyStringFallback for String {
    fn or_zero(self) -> String {
        if self.is_empty() {
            "0".to_string()
        } else {
            self
        }
    }
}

/// Spot-only Hyperliquid exchange provider. Quotes and executes orders using
/// the `hyperliquid-client` crate; signing happens at submit time in the
/// custody executor so the EIP-712 nonce binds to the signature.
///
/// Two execution paths:
/// - **Same-token no-op** — input and output resolve to the same HL spot
///   token (e.g. UBTC→UBTC). Emits `ProviderOnly` with a completed operation.
/// - **Cross-token single-leg** — one side is USDC, the other is a non-USDC
///   HL spot token. Emits one `CustodyActions` intent carrying a single GTC
///   `{base}/USDC` order against the hydrated HL spot custody vault. The
///   planner composes these into multi-hop routes (UBTC→USDC, USDC→UETH)
///   when both sides are non-USDC.
#[derive(Clone)]
pub struct HyperliquidProvider {
    network: HyperliquidCallNetwork,
    asset_registry: Arc<AssetRegistry>,
    http: HyperliquidHttpClient,
    target_base_url: String,
    order_timeout_ms: u64,
    spot_meta: Arc<OnceCell<SpotMeta>>,
}

/// HL spot pairs always quote against USDC. Hardcoding the symbol avoids
/// redundant registry lookups and clarifies the leg-composition invariant.
const HL_QUOTE_SYMBOL: &str = "USDC";

/// HL's 5-significant-figure cap on spot prices (per exchange rules).
const HL_SPOT_PRICE_SIG_FIGS: i32 = 5;

/// HL's max decimal places for any spot amount (sz, px) — prices are further
/// constrained to `HL_SPOT_MAX_DECIMALS - sz_decimals`.
const HL_SPOT_MAX_DECIMALS: i32 = 8;
const DEFAULT_HYPERLIQUID_ORDER_TIMEOUT_MS: u64 = 30_000;

impl HyperliquidProvider {
    #[must_use]
    pub fn new(
        base_url: impl Into<String>,
        network: HyperliquidCallNetwork,
        asset_registry: Arc<AssetRegistry>,
        order_timeout_ms: u64,
    ) -> Self {
        let target_base_url = normalize_base_url(base_url);
        let http = HyperliquidHttpClient::new(rustls_http_client(), target_base_url.clone());
        Self {
            network,
            asset_registry,
            http,
            target_base_url,
            order_timeout_ms,
            spot_meta: Arc::new(OnceCell::new()),
        }
    }

    /// Resolve a router-side [`DepositAsset`] to its HL spot entry: token
    /// symbol (e.g. `"UBTC"`) and the router-side decimals (used for
    /// wire-amount → natural-unit conversion when computing `limit_px`).
    fn resolve_hl_entry(
        &self,
        asset: &DepositAsset,
        capability: ProviderAssetCapability,
    ) -> ProviderResult<(String, u8)> {
        self.asset_registry
            .provider_asset(ProviderId::Hyperliquid, asset, capability)
            .map(|entry| (entry.provider_asset.clone(), entry.decimals))
            .ok_or_else(|| {
                format!(
                    "hyperliquid does not register {} on {} for {capability:?}",
                    asset.asset, asset.chain
                )
            })
    }

    /// Fetch `/info spotMeta` once per process — pairs + token decimals are
    /// stable, and every trade needs this to compute the wire asset index and
    /// price/size formatting.
    async fn spot_meta(&self) -> ProviderResult<&SpotMeta> {
        self.spot_meta
            .get_or_try_init(|| async {
                let req = json!({ "type": "spotMeta" });
                let meta: SpotMeta = self
                    .http
                    .post_json("/info", &req)
                    .await
                    .map_err(|err| format!("fetch hyperliquid spotMeta: {err}"))?;
                Ok::<_, String>(meta)
            })
            .await
    }

    /// Locate the spot pair where `base_symbol` is the base and `quote_symbol`
    /// is the quote. Returns `(wire_asset_index, pair_name, base_token,
    /// quote_token)` so callers can size sz/px using `sz_decimals` and use
    /// the exchange's canonical pair identifier for `l2Book`.
    async fn resolve_spot_pair(
        &self,
        base_symbol: &str,
        quote_symbol: &str,
    ) -> ProviderResult<(u32, String, TokenInfo, TokenInfo)> {
        let meta = self.spot_meta().await?;
        let base = meta
            .tokens
            .iter()
            .find(|t| t.name == base_symbol)
            .ok_or_else(|| format!("hyperliquid spotMeta missing token {base_symbol}"))?;
        let quote = meta
            .tokens
            .iter()
            .find(|t| t.name == quote_symbol)
            .ok_or_else(|| format!("hyperliquid spotMeta missing token {quote_symbol}"))?;
        let pair = meta
            .universe
            .iter()
            .find(|p| p.tokens[0] == base.index && p.tokens[1] == quote.index)
            .ok_or_else(|| {
                format!("hyperliquid spotMeta missing pair {base_symbol}/{quote_symbol}")
            })?;
        let wire_asset = SPOT_ASSET_INDEX_OFFSET + pair.index as u32;
        Ok((wire_asset, pair.name.clone(), base.clone(), quote.clone()))
    }

    /// Fetch `/info l2Book` for a spot pair (e.g. `"UBTC/USDC"`). HL returns a
    /// snapshot with bids at `levels[0]` (descending px) and asks at
    /// `levels[1]` (ascending px).
    async fn fetch_l2_book(&self, pair_coin: &str) -> ProviderResult<L2BookSnapshot> {
        let req = json!({ "type": "l2Book", "coin": pair_coin });
        self.http
            .post_json::<_, L2BookSnapshot>("/info", &req)
            .await
            .map_err(|err| format!("fetch hyperliquid l2Book {pair_coin}: {err}"))
    }

    /// Single-leg cross-token quote (one side is USDC). Walks the
    /// `{base}/USDC` book to compute the other side of the trade.
    async fn quote_single_leg(
        &self,
        request: &ExchangeQuoteRequest,
        input_token: &str,
        input_decimals: u8,
        output_token: &str,
        output_decimals: u8,
    ) -> ProviderResult<ExchangeQuote> {
        let (base_symbol, _, is_buy) =
            classify_hl_leg(input_token, input_decimals, output_token, output_decimals)?;
        let (_, pair_name, base_meta, _) = self
            .resolve_spot_pair(&base_symbol, HL_QUOTE_SYMBOL)
            .await?;
        let book = self.fetch_l2_book(&pair_name).await?;

        let (amount_in_wire, amount_out_wire) = simulate_leg(
            is_buy,
            input_decimals,
            output_decimals,
            base_meta.sz_decimals,
            &request.order_kind,
            &book,
        )?;

        let leg = hl_leg_descriptor(
            &request.input_asset,
            &request.output_asset,
            &request.order_kind,
            &amount_in_wire,
            &amount_out_wire,
        );
        let (min_amount_out, max_amount_in) = slippage_bounds_from_request(&request.order_kind);

        Ok(ExchangeQuote {
            provider_id: self.id().to_string(),
            amount_in: amount_in_wire,
            amount_out: amount_out_wire,
            min_amount_out,
            max_amount_in,
            provider_quote: json!({
                "schema_version": 1,
                "kind": "spot_cross_token",
                "legs": [leg],
            }),
            expires_at: Utc::now() + chrono::Duration::minutes(10),
        })
    }

    /// Two-leg cross-token quote (neither side is USDC). Chains through USDC
    /// by walking the `{input}/USDC` and `{output}/USDC` books in sequence —
    /// forward for `exact_in`, backward for `exact_out`.
    async fn quote_two_leg(
        &self,
        request: &ExchangeQuoteRequest,
        input_token: &str,
        input_decimals: u8,
        output_token: &str,
        output_decimals: u8,
    ) -> ProviderResult<ExchangeQuote> {
        let (_, input_pair_name, input_base_meta, _) =
            self.resolve_spot_pair(input_token, HL_QUOTE_SYMBOL).await?;
        let (_, output_pair_name, output_base_meta, _) = self
            .resolve_spot_pair(output_token, HL_QUOTE_SYMBOL)
            .await?;
        let sell_book = self.fetch_l2_book(&input_pair_name).await?;
        let buy_book = self.fetch_l2_book(&output_pair_name).await?;

        let usdc_asset = DepositAsset {
            chain: ChainId::parse("hyperliquid").expect("static chain id"),
            asset: AssetId::Native,
        };
        let (usdc_token, usdc_decimals) =
            self.resolve_hl_entry(&usdc_asset, ProviderAssetCapability::ExchangeInput)?;
        debug_assert_eq!(usdc_token, HL_QUOTE_SYMBOL);

        let (leg1_in, leg1_out, leg2_in, leg2_out) = match &request.order_kind {
            MarketOrderKind::ExactIn {
                amount_in,
                min_amount_out,
            } => {
                // Leg 1: SELL input for USDC (exact_in on the input amount).
                let (_, usdc_from_leg1) = simulate_leg(
                    /* is_buy = */ false,
                    input_decimals,
                    usdc_decimals,
                    input_base_meta.sz_decimals,
                    &MarketOrderKind::ExactIn {
                        amount_in: amount_in.clone(),
                        // The intermediate slippage bound is enforced by the
                        // outer user bound on the final output.
                        min_amount_out: "1".to_string(),
                    },
                    &sell_book,
                )?;
                // Leg 2: BUY output with all the USDC from leg 1 (exact_in).
                let (_, output_from_leg2) = simulate_leg(
                    /* is_buy = */ true,
                    usdc_decimals,
                    output_decimals,
                    output_base_meta.sz_decimals,
                    &MarketOrderKind::ExactIn {
                        amount_in: usdc_from_leg1.clone(),
                        min_amount_out: min_amount_out.clone(),
                    },
                    &buy_book,
                )?;
                (
                    amount_in.clone(),
                    usdc_from_leg1.clone(),
                    usdc_from_leg1,
                    output_from_leg2,
                )
            }
            MarketOrderKind::ExactOut {
                amount_out,
                max_amount_in,
            } => {
                // Leg 2: BUY `amount_out` output — how much USDC does it need?
                let (usdc_for_leg2, _) = simulate_leg(
                    /* is_buy = */ true,
                    usdc_decimals,
                    output_decimals,
                    output_base_meta.sz_decimals,
                    &MarketOrderKind::ExactOut {
                        amount_out: amount_out.clone(),
                        max_amount_in: PRACTICAL_USDC_MAX_WIRE_STR.to_string(),
                    },
                    &buy_book,
                )?;
                // Leg 1: SELL input to cover `usdc_for_leg2` — how much input?
                let (input_for_leg1, _) = simulate_leg(
                    /* is_buy = */ false,
                    input_decimals,
                    usdc_decimals,
                    input_base_meta.sz_decimals,
                    &MarketOrderKind::ExactOut {
                        amount_out: usdc_for_leg2.clone(),
                        max_amount_in: max_amount_in.clone(),
                    },
                    &sell_book,
                )?;
                (
                    input_for_leg1,
                    usdc_for_leg2.clone(),
                    usdc_for_leg2,
                    amount_out.clone(),
                )
            }
        };

        let leg1_kind = match &request.order_kind {
            MarketOrderKind::ExactIn { .. } => MarketOrderKind::ExactIn {
                amount_in: leg1_in.clone(),
                min_amount_out: "1".to_string(),
            },
            MarketOrderKind::ExactOut { .. } => MarketOrderKind::ExactOut {
                amount_out: leg1_out.clone(),
                max_amount_in: leg1_in.clone(),
            },
        };
        let leg2_kind = match &request.order_kind {
            MarketOrderKind::ExactIn { min_amount_out, .. } => MarketOrderKind::ExactIn {
                amount_in: leg2_in.clone(),
                min_amount_out: min_amount_out.clone(),
            },
            MarketOrderKind::ExactOut { amount_out, .. } => MarketOrderKind::ExactOut {
                amount_out: amount_out.clone(),
                max_amount_in: leg2_in.clone(),
            },
        };

        let leg1 = hl_leg_descriptor(
            &request.input_asset,
            &usdc_asset,
            &leg1_kind,
            &leg1_in,
            &leg1_out,
        );
        let leg2 = hl_leg_descriptor(
            &usdc_asset,
            &request.output_asset,
            &leg2_kind,
            &leg2_in,
            &leg2_out,
        );

        let (min_amount_out, max_amount_in) = slippage_bounds_from_request(&request.order_kind);

        Ok(ExchangeQuote {
            provider_id: self.id().to_string(),
            amount_in: leg1_in,
            amount_out: leg2_out,
            min_amount_out,
            max_amount_in,
            provider_quote: json!({
                "schema_version": 1,
                "kind": "spot_cross_token",
                "legs": [leg1, leg2],
            }),
            expires_at: Utc::now() + chrono::Duration::minutes(10),
        })
    }

    async fn execute_limit_order(
        &self,
        step: &HyperliquidLimitOrderStepRequest,
    ) -> ProviderResult<ProviderExecutionIntent> {
        let input_asset = DepositAsset {
            chain: ChainId::parse(&step.input_asset.chain_id)
                .map_err(|err| format!("invalid input_asset chain: {err}"))?,
            asset: AssetId::parse(&step.input_asset.asset)
                .map_err(|err| format!("invalid input_asset asset: {err}"))?,
        };
        let output_asset = DepositAsset {
            chain: ChainId::parse(&step.output_asset.chain_id)
                .map_err(|err| format!("invalid output_asset chain: {err}"))?,
            asset: AssetId::parse(&step.output_asset.asset)
                .map_err(|err| format!("invalid output_asset asset: {err}"))?,
        };

        let (input_token, input_decimals) =
            self.resolve_hl_entry(&input_asset, ProviderAssetCapability::ExchangeInput)?;
        let (output_token, output_decimals) =
            self.resolve_hl_entry(&output_asset, ProviderAssetCapability::ExchangeOutput)?;
        let (base_symbol, _base_decimals, is_buy) =
            classify_hl_leg(&input_token, input_decimals, &output_token, output_decimals)?;

        let vault_id = step.hyperliquid_custody_vault_id.ok_or_else(|| {
            "hyperliquid limit order: hyperliquid_custody_vault_id must be hydrated".to_string()
        })?;
        let vault_address = step
            .hyperliquid_custody_vault_address
            .clone()
            .ok_or_else(|| {
                "hyperliquid limit order: hyperliquid_custody_vault_address must be hydrated"
                    .to_string()
            })?;
        Address::from_str(&vault_address).map_err(|err| {
            format!("hyperliquid limit order: invalid vault_address {vault_address}: {err}")
        })?;

        let (wire_asset, _pair_name, base_meta, _quote_meta) = self
            .resolve_spot_pair(&base_symbol, HL_QUOTE_SYMBOL)
            .await?;
        let base_sz = compute_base_sz(
            is_buy,
            &step.amount_in,
            input_decimals,
            &step.amount_out,
            output_decimals,
            base_meta.sz_decimals,
        )?;
        let (order_kind, min_amount_out, max_amount_in) = if is_buy {
            ("exact_out", None, Some(step.amount_in.as_str()))
        } else {
            ("exact_in", Some(step.amount_out.as_str()), None)
        };
        let limit_px = compute_limit_px(LimitPxInput {
            is_buy,
            order_kind,
            amount_in_wire: &step.amount_in,
            input_decimals,
            amount_out_wire: &step.amount_out,
            output_decimals,
            min_amount_out_wire: min_amount_out,
            max_amount_in_wire: max_amount_in,
            base_sz_decimals: base_meta.sz_decimals,
        })?;

        let order = OrderRequest {
            asset: wire_asset,
            is_buy,
            limit_px: limit_px.clone(),
            sz: base_sz.clone(),
            reduce_only: false,
            order_type: Order::Limit(Limit {
                tif: Tif::Gtc.as_wire().to_string(),
            }),
            cloid: None,
        };
        let action = HyperliquidActions::Order(BulkOrder {
            orders: vec![order],
            grouping: "na".to_string(),
        });
        let mut actions = Vec::new();
        if step.prefund_from_withdrawable {
            let transfer_amount = format_hyperliquid_amount(&step.amount_in, input_decimals)?;
            actions.push(CustodyAction::Call(ChainCall::Hyperliquid(
                HyperliquidCall {
                    target_base_url: self.target_base_url.clone(),
                    network: self.network,
                    vault_address: None,
                    payload: HyperliquidCallPayload::UsdClassTransfer {
                        amount: transfer_amount,
                        to_perp: false,
                    },
                },
            )));
        }
        actions.push(CustodyAction::Call(ChainCall::Hyperliquid(
            HyperliquidCall {
                target_base_url: self.target_base_url.clone(),
                network: self.network,
                vault_address: None,
                payload: HyperliquidCallPayload::L1Action { action },
            },
        )));

        let operation_request = json!({
            "pair": format!("{base_symbol}/{HL_QUOTE_SYMBOL}"),
            "asset_index": wire_asset,
            "is_buy": is_buy,
            "sz": base_sz,
            "limit_px": limit_px,
            "tif": Tif::Gtc.as_wire(),
            "user": vault_address,
            "hyperliquid_custody_vault_id": vault_id,
            "prefund_from_withdrawable": step.prefund_from_withdrawable,
            "target_base_url": self.target_base_url.clone(),
            "network": self.network,
            "residual_policy": step.residual_policy,
        });

        Ok(ProviderExecutionIntent::CustodyActions {
            custody_vault_id: vault_id,
            actions,
            provider_context: operation_request.clone(),
            state: ProviderExecutionState {
                operation: Some(ProviderOperationIntent {
                    operation_type: ProviderOperationType::HyperliquidLimitOrder,
                    status: ProviderOperationStatus::Submitted,
                    provider_ref: None,
                    request: Some(operation_request),
                    response: None,
                    observed_state: None,
                }),
                addresses: vec![],
            },
        })
    }
}

fn rustls_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .use_rustls_tls()
        .build()
        .expect("failed to construct reqwest client with rustls")
}

#[derive(Clone)]
pub struct VeloraProvider {
    base_url: String,
    partner: Option<String>,
    http: reqwest::Client,
}

impl VeloraProvider {
    #[must_use]
    pub fn new(base_url: impl Into<String>, partner: Option<String>) -> Self {
        Self {
            base_url: normalize_base_url(base_url),
            partner: partner
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            http: rustls_http_client(),
        }
    }

    async fn fetch_price_route(
        &self,
        request: &ExchangeQuoteRequest,
        src_token: &str,
        dest_token: &str,
        src_decimals: u8,
        dest_decimals: u8,
        network: u64,
    ) -> ProviderResult<Option<Value>> {
        let (side, amount) = match &request.order_kind {
            MarketOrderKind::ExactIn { amount_in, .. } => ("SELL", amount_in.clone()),
            MarketOrderKind::ExactOut { amount_out, .. } => ("BUY", amount_out.clone()),
        };
        let mut query = vec![
            ("srcToken", src_token.to_string()),
            ("destToken", dest_token.to_string()),
            ("srcDecimals", src_decimals.to_string()),
            ("destDecimals", dest_decimals.to_string()),
            ("amount", amount),
            ("side", side.to_string()),
            ("network", network.to_string()),
            ("version", VELORA_API_VERSION.to_string()),
            ("ignoreBadUsdPrice", "true".to_string()),
            ("excludeRFQ", "true".to_string()),
        ];
        if let Some(sender) = request.sender_address.as_ref() {
            query.push(("userAddress", sender.clone()));
        }
        if !request.recipient_address.trim().is_empty() {
            query.push(("receiver", request.recipient_address.clone()));
        }
        if let Some(partner) = self.partner.as_ref() {
            query.push(("partner", partner.clone()));
        }

        let url = format!("{}/prices", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&query)
            .send()
            .await
            .map_err(|err| format!("velora price request failed: {err}"))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| format!("velora price response body failed: {err}"))?;
        if !status.is_success() {
            if status.is_client_error() && velora_quote_error_is_no_route(&body) {
                return Ok(None);
            }
            return Err(format!("velora price request failed with {status}: {body}"));
        }
        let body: Value = serde_json::from_str(&body)
            .map_err(|err| format!("velora price response was not JSON: {err}"))?;
        let Some(price_route) = body.get("priceRoute").cloned() else {
            return Err(format!("velora price response missing priceRoute: {body}"));
        };
        Ok(Some(price_route))
    }

    async fn build_transaction(
        &self,
        step: &UniversalRouterSwapStepRequest,
        src_token: &str,
        dest_token: &str,
        network: u64,
        side: &str,
    ) -> ProviderResult<Value> {
        let source_address = step
            .source_custody_vault_address
            .as_deref()
            .ok_or_else(|| {
                "velora universal router step missing source_custody_vault_address".to_string()
            })?;
        let mut body = json!({
            "srcToken": src_token,
            "srcDecimals": step.input_decimals,
            "destToken": dest_token,
            "destDecimals": step.output_decimals,
            "priceRoute": step.price_route,
            "userAddress": source_address,
            "receiver": step.recipient_address,
            "partner": self.partner.as_deref().unwrap_or(VELORA_DEFAULT_PARTNER),
            "slippage": step.slippage_bps.unwrap_or(VELORA_DEFAULT_SLIPPAGE_BPS),
            "side": side,
        });
        match side {
            "SELL" => set_json_value(&mut body, "srcAmount", json!(step.amount_in)),
            "BUY" => set_json_value(&mut body, "destAmount", json!(step.amount_out)),
            other => return Err(format!("unsupported velora side {other:?}")),
        }

        let url = format!(
            "{}/transactions/{network}?ignoreChecks=true&ignoreGasEstimate=true",
            self.base_url
        );
        let response = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|err| format!("velora transaction request failed: {err}"))?;
        let status = response.status();
        let response_body = response
            .text()
            .await
            .map_err(|err| format!("velora transaction response body failed: {err}"))?;
        if !status.is_success() {
            return Err(format!(
                "velora transaction request failed with {status}: {response_body}"
            ));
        }
        serde_json::from_str(&response_body)
            .map_err(|err| format!("velora transaction response was not JSON: {err}"))
    }
}

impl ExchangeProvider for VeloraProvider {
    fn id(&self) -> &str {
        "velora"
    }

    fn quote_trade<'a>(
        &'a self,
        request: ExchangeQuoteRequest,
    ) -> ProviderFuture<'a, Option<ExchangeQuote>> {
        Box::pin(async move {
            if request.input_asset.chain != request.output_asset.chain {
                return Ok(None);
            }
            let Some(network) = request.input_asset.chain.evm_chain_id() else {
                return Ok(None);
            };
            let Some(src_decimals) =
                velora_asset_decimals(&request.input_asset, request.input_decimals)
            else {
                return Ok(None);
            };
            let Some(dest_decimals) =
                velora_asset_decimals(&request.output_asset, request.output_decimals)
            else {
                return Ok(None);
            };
            let Some(src_token) = velora_token_address(&request.input_asset) else {
                return Ok(None);
            };
            let Some(dest_token) = velora_token_address(&request.output_asset) else {
                return Ok(None);
            };

            let Some(price_route) = self
                .fetch_price_route(
                    &request,
                    &src_token,
                    &dest_token,
                    src_decimals,
                    dest_decimals,
                    network,
                )
                .await?
            else {
                return Ok(None);
            };
            let amount_in = velora_price_route_string(&price_route, "srcAmount")?;
            let amount_out = velora_price_route_string(&price_route, "destAmount")?;
            let (min_amount_out, max_amount_in) = slippage_bounds_from_request(&request.order_kind);
            let slippage_bps = velora_slippage_bps(&request.order_kind, &amount_in, &amount_out)?;

            Ok(Some(ExchangeQuote {
                provider_id: self.id().to_string(),
                amount_in: amount_in.clone(),
                amount_out: amount_out.clone(),
                min_amount_out,
                max_amount_in,
                provider_quote: velora_quote_descriptor(
                    &request,
                    network,
                    &src_token,
                    &dest_token,
                    src_decimals,
                    dest_decimals,
                    slippage_bps,
                    price_route,
                    &amount_in,
                    &amount_out,
                ),
                expires_at: Utc::now() + chrono::Duration::minutes(5),
            }))
        })
    }

    fn execute_trade<'a>(
        &'a self,
        request: &'a ExchangeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            let ExchangeExecutionRequest::UniversalRouterSwap(step) = request else {
                return Err(format!(
                    "velora cannot execute exchange request kind {}",
                    request.kind()
                ));
            };
            let input_asset = step.input_asset.deposit_asset()?;
            let output_asset = step.output_asset.deposit_asset()?;
            if input_asset.chain != output_asset.chain {
                return Err("velora swaps must stay on one EVM chain".to_string());
            }
            let network = input_asset
                .chain
                .evm_chain_id()
                .ok_or_else(|| "velora swaps require an EVM chain".to_string())?;
            let src_token = velora_token_address(&input_asset)
                .ok_or_else(|| "velora swaps require native or ERC-20 input asset".to_string())?;
            let dest_token = velora_token_address(&output_asset)
                .ok_or_else(|| "velora swaps require native or ERC-20 output asset".to_string())?;
            let source_vault_id = step.source_custody_vault_id.ok_or_else(|| {
                "velora universal router step missing source_custody_vault_id".to_string()
            })?;
            let side = match step.order_kind.as_str() {
                "exact_in" => "SELL",
                "exact_out" => "BUY",
                other => return Err(format!("unsupported velora order_kind {other:?}")),
            };
            let transaction = self
                .build_transaction(step, &src_token, &dest_token, network, side)
                .await?;
            let swap_to = velora_response_string(&transaction, "to")?;
            let swap_data = velora_response_string(&transaction, "data")?;
            let swap_value = velora_response_optional_u256(&transaction, "value")?
                .unwrap_or(U256::ZERO)
                .to_string();

            let mut actions = Vec::new();
            if let AssetId::Reference(token_address) = &input_asset.asset {
                let spender = velora_price_route_string(&step.price_route, "tokenTransferProxy")
                    .or_else(|_| velora_price_route_string(&step.price_route, "contractAddress"))
                    .unwrap_or_else(|_| swap_to.clone());
                let approve_amount = step.max_amount_in.as_deref().unwrap_or(&step.amount_in);
                actions.push(CustodyAction::Call(ChainCall::Evm(EvmCall {
                    to_address: token_address.clone(),
                    value: "0".to_string(),
                    calldata: encode_erc20_approve(&spender, approve_amount)?,
                })));
            }
            actions.push(CustodyAction::Call(ChainCall::Evm(EvmCall {
                to_address: swap_to,
                value: swap_value,
                calldata: swap_data,
            })));

            Ok(ProviderExecutionIntent::CustodyActions {
                custody_vault_id: source_vault_id,
                actions,
                provider_context: json!({
                    "kind": "velora_universal_router_swap",
                    "network": network,
                    "order_kind": step.order_kind,
                }),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::UniversalRouterSwap,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: None,
                        request: Some(
                            serde_json::to_value(step)
                                .map_err(|err| format!("serialize velora step request: {err}"))?,
                        ),
                        response: Some(transaction),
                        observed_state: Some(json!({
                            "kind": "velora_universal_router_swap",
                            "network": network,
                        })),
                    }),
                    addresses: vec![],
                },
            })
        })
    }

    fn post_execute<'a>(
        &'a self,
        provider_context: &'a Value,
        receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async move {
            let tx_hashes: Vec<String> = receipts
                .iter()
                .map(|receipt| receipt.tx_hash.clone())
                .collect();
            let tx_hash = tx_hashes.last().cloned();
            Ok(ProviderExecutionStatePatch {
                provider_ref: tx_hash.clone(),
                observed_state: Some(json!({
                    "kind": "velora_universal_router_swap",
                    "provider_context": provider_context,
                    "tx_hash": tx_hash,
                    "tx_hashes": tx_hashes,
                })),
                response: None,
                status: Some(ProviderOperationStatus::Completed),
            })
        })
    }

    fn observe_trade_operation<'a>(
        &'a self,
        request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async move {
            Ok(Some(ProviderOperationObservation {
                status: ProviderOperationStatus::Completed,
                provider_ref: request.provider_ref.clone(),
                observed_state: request.observed_state.clone(),
                response: Some(request.response.clone()),
                tx_hash: request.provider_ref.clone(),
                error: None,
            }))
        })
    }
}

impl ExchangeProvider for HyperliquidProvider {
    fn id(&self) -> &str {
        "hyperliquid"
    }

    fn quote_trade<'a>(
        &'a self,
        request: ExchangeQuoteRequest,
    ) -> ProviderFuture<'a, Option<ExchangeQuote>> {
        Box::pin(async move {
            let Ok((input_token, input_decimals)) =
                self.resolve_hl_entry(&request.input_asset, ProviderAssetCapability::ExchangeInput)
            else {
                return Ok(None);
            };
            let Ok((output_token, output_decimals)) = self.resolve_hl_entry(
                &request.output_asset,
                ProviderAssetCapability::ExchangeOutput,
            ) else {
                return Ok(None);
            };

            if input_token == output_token {
                return Ok(Some(hyperliquid_no_op_quote(
                    &request,
                    self.id(),
                    &input_token,
                )));
            }

            let quote = if matches_quote(&input_token) || matches_quote(&output_token) {
                self.quote_single_leg(
                    &request,
                    &input_token,
                    input_decimals,
                    &output_token,
                    output_decimals,
                )
                .await?
            } else {
                self.quote_two_leg(
                    &request,
                    &input_token,
                    input_decimals,
                    &output_token,
                    output_decimals,
                )
                .await?
            };
            Ok(Some(quote))
        })
    }

    fn execute_trade<'a>(
        &'a self,
        request: &'a ExchangeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            if let ExchangeExecutionRequest::HyperliquidLimitOrder(step) = request {
                return self.execute_limit_order(step).await;
            }
            let ExchangeExecutionRequest::HyperliquidTrade(step) = request else {
                return Err(format!(
                    "hyperliquid cannot execute exchange request kind {}",
                    request.kind()
                ));
            };

            let input_asset = DepositAsset {
                chain: ChainId::parse(&step.input_asset.chain_id)
                    .map_err(|err| format!("invalid input_asset chain: {err}"))?,
                asset: AssetId::parse(&step.input_asset.asset)
                    .map_err(|err| format!("invalid input_asset asset: {err}"))?,
            };
            let output_asset = DepositAsset {
                chain: ChainId::parse(&step.output_asset.chain_id)
                    .map_err(|err| format!("invalid output_asset chain: {err}"))?,
                asset: AssetId::parse(&step.output_asset.asset)
                    .map_err(|err| format!("invalid output_asset asset: {err}"))?,
            };

            let (input_token, input_decimals) =
                self.resolve_hl_entry(&input_asset, ProviderAssetCapability::ExchangeInput)?;
            let (output_token, output_decimals) =
                self.resolve_hl_entry(&output_asset, ProviderAssetCapability::ExchangeOutput)?;

            if input_token == output_token {
                return Ok(ProviderExecutionIntent::ProviderOnly {
                    response: json!({
                        "kind": "hyperliquid_trade",
                        "mode": "no_op",
                        "token": input_token,
                        "amount_in": step.amount_in,
                        "amount_out": step.amount_out,
                    }),
                    state: ProviderExecutionState {
                        operation: Some(ProviderOperationIntent {
                            operation_type: ProviderOperationType::HyperliquidTrade,
                            status: ProviderOperationStatus::Completed,
                            provider_ref: None,
                            request: Some(json!({
                                "mode": "no_op",
                                "token": input_token,
                                "order_kind": step.order_kind,
                            })),
                            response: Some(json!({ "kind": "no_op" })),
                            observed_state: Some(json!({
                                "kind": "no_op",
                                "token": input_token,
                                "amount_in": step.amount_in,
                                "amount_out": step.amount_out,
                            })),
                        }),
                        addresses: vec![],
                    },
                });
            }

            // Cross-token path: one side must be USDC (HL's only quote
            // currency); the other side is the base. The planner composes
            // non-USDC↔non-USDC routes as two sequential HL trade steps.
            let (base_symbol, _base_decimals, is_buy) =
                classify_hl_leg(&input_token, input_decimals, &output_token, output_decimals)?;

            let vault_id = step.hyperliquid_custody_vault_id.ok_or_else(|| {
                "hyperliquid trade: hyperliquid_custody_vault_id must be hydrated".to_string()
            })?;
            let vault_address =
                step.hyperliquid_custody_vault_address
                    .clone()
                    .ok_or_else(|| {
                        "hyperliquid trade: hyperliquid_custody_vault_address must be hydrated"
                            .to_string()
                    })?;
            Address::from_str(&vault_address).map_err(|err| {
                format!("hyperliquid trade: invalid vault_address {vault_address}: {err}")
            })?;

            let (wire_asset, _pair_name, base_meta, _quote_meta) = self
                .resolve_spot_pair(&base_symbol, HL_QUOTE_SYMBOL)
                .await?;

            let base_sz = compute_base_sz(
                is_buy,
                &step.amount_in,
                input_decimals,
                &step.amount_out,
                output_decimals,
                base_meta.sz_decimals,
            )?;
            let limit_px = compute_limit_px(LimitPxInput {
                is_buy,
                order_kind: &step.order_kind,
                amount_in_wire: &step.amount_in,
                input_decimals,
                amount_out_wire: &step.amount_out,
                output_decimals,
                min_amount_out_wire: step.min_amount_out.as_deref(),
                max_amount_in_wire: step.max_amount_in.as_deref(),
                base_sz_decimals: base_meta.sz_decimals,
            })?;

            let order = OrderRequest {
                asset: wire_asset,
                is_buy,
                limit_px: limit_px.clone(),
                sz: base_sz.clone(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: Tif::Gtc.as_wire().to_string(),
                }),
                cloid: None,
            };
            let action = HyperliquidActions::Order(BulkOrder {
                orders: vec![order],
                grouping: "na".to_string(),
            });
            let now_ms = Utc::now().timestamp_millis().max(0) as u64;
            let timeout_at_ms = now_ms.saturating_add(self.order_timeout_ms);
            let mut actions = Vec::new();
            if step.prefund_from_withdrawable {
                let transfer_amount = format_hyperliquid_amount(&step.amount_in, input_decimals)?;
                actions.push(CustodyAction::Call(ChainCall::Hyperliquid(
                    HyperliquidCall {
                        target_base_url: self.target_base_url.clone(),
                        network: self.network,
                        vault_address: None,
                        payload: HyperliquidCallPayload::UsdClassTransfer {
                            amount: transfer_amount,
                            to_perp: false,
                        },
                    },
                )));
            }
            let order_action = CustodyAction::Call(ChainCall::Hyperliquid(HyperliquidCall {
                target_base_url: self.target_base_url.clone(),
                network: self.network,
                vault_address: None,
                payload: HyperliquidCallPayload::L1Action { action },
            }));
            actions.push(order_action);

            let operation_request = json!({
                "pair": format!("{base_symbol}/{HL_QUOTE_SYMBOL}"),
                "asset_index": wire_asset,
                "is_buy": is_buy,
                "sz": base_sz,
                "limit_px": limit_px,
                "tif": Tif::Gtc.as_wire(),
                "user": vault_address,
                "timeout_at_ms": timeout_at_ms,
                "order_timeout_ms": self.order_timeout_ms,
                "hyperliquid_custody_vault_id": vault_id,
                "prefund_from_withdrawable": step.prefund_from_withdrawable,
                "target_base_url": self.target_base_url.clone(),
                "network": self.network,
            });

            Ok(ProviderExecutionIntent::CustodyActions {
                custody_vault_id: vault_id,
                actions,
                provider_context: operation_request.clone(),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::HyperliquidTrade,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: None,
                        request: Some(operation_request),
                        response: None,
                        observed_state: None,
                    }),
                    addresses: vec![],
                },
            })
        })
    }

    fn post_execute<'a>(
        &'a self,
        _provider_context: &'a Value,
        receipts: &'a [CustodyActionReceipt],
    ) -> ProviderFuture<'a, ProviderExecutionStatePatch> {
        Box::pin(async move {
            let receipt = receipts.last().ok_or_else(|| {
                "hyperliquid post_execute: no custody action receipts".to_string()
            })?;
            let response = receipt.response.as_ref().ok_or_else(|| {
                "hyperliquid post_execute: custody receipt missing HL response body".to_string()
            })?;

            let status_entry = response
                .pointer("/response/data/statuses/0")
                .ok_or_else(|| {
                    format!(
                        "hyperliquid post_execute: response missing /response/data/statuses/0: {response}"
                    )
                })?;

            if let Some(filled) = status_entry.get("filled") {
                let oid = filled
                    .get("oid")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| "hyperliquid post_execute: filled missing oid".to_string())?;
                return Ok(ProviderExecutionStatePatch {
                    provider_ref: Some(oid.to_string()),
                    observed_state: Some(json!({
                        "kind": "filled",
                        "oid": oid,
                        "total_sz": filled.get("totalSz").cloned().unwrap_or(Value::Null),
                        "avg_px": filled.get("avgPx").cloned().unwrap_or(Value::Null),
                        "tx_hash": &receipt.tx_hash,
                    })),
                    response: None,
                    status: Some(ProviderOperationStatus::Completed),
                });
            }

            if let Some(resting) = status_entry.get("resting") {
                let oid = resting
                    .get("oid")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| "hyperliquid post_execute: resting missing oid".to_string())?;
                return Ok(ProviderExecutionStatePatch {
                    provider_ref: Some(oid.to_string()),
                    observed_state: Some(json!({
                        "kind": "resting",
                        "oid": oid,
                        "tx_hash": &receipt.tx_hash,
                    })),
                    response: None,
                    status: Some(ProviderOperationStatus::WaitingExternal),
                });
            }

            if let Some(error) = status_entry.get("error") {
                return Ok(ProviderExecutionStatePatch {
                    provider_ref: None,
                    observed_state: Some(json!({
                        "kind": "error",
                        "error": error,
                        "tx_hash": &receipt.tx_hash,
                    })),
                    response: None,
                    status: Some(ProviderOperationStatus::Failed),
                });
            }

            Err(format!(
                "hyperliquid post_execute: unexpected status entry shape: {status_entry}"
            ))
        })
    }

    fn observe_trade_operation<'a>(
        &'a self,
        request: ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<ProviderOperationObservation>> {
        Box::pin(async move {
            let Some(provider_ref) = request.provider_ref.as_deref() else {
                return Ok(None);
            };
            let oid: u64 = provider_ref
                .parse()
                .map_err(|err| format!("hyperliquid observe: invalid oid {provider_ref}: {err}"))?;
            let user_str = request
                .request
                .get("user")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    "hyperliquid observe: operation request missing user address".to_string()
                })?;
            let user = Address::from_str(user_str).map_err(|err| {
                format!("hyperliquid observe: invalid user address {user_str}: {err}")
            })?;

            let info_req = json!({
                "type": "orderStatus",
                "user": format!("{user:?}"),
                "oid": oid,
            });
            let order_status: OrderStatusResponse =
                self.http
                    .post_json("/info", &info_req)
                    .await
                    .map_err(|err| format!("hyperliquid /info orderStatus: {err}"))?;

            let observed_state = serde_json::to_value(&order_status).map_err(|err| {
                format!("hyperliquid observe: serialize orderStatus response: {err}")
            })?;
            let (status, tx_hash) = match order_status.status.as_str() {
                // HL returns "order" when the oid is still live/resting.
                "order" => (ProviderOperationStatus::WaitingExternal, None),
                "unknownOid" => (ProviderOperationStatus::WaitingExternal, None),
                _ => {
                    let envelope_status = order_status
                        .order
                        .as_ref()
                        .map(|env| env.status.as_str())
                        .unwrap_or("");
                    let mapped = match envelope_status {
                        "filled" => ProviderOperationStatus::Completed,
                        "canceled"
                        | "scheduledCancel"
                        | "marginCanceled"
                        | "rejected"
                        | "vaultWithdrawal"
                        | "vaultWithdrawalCanceled" => ProviderOperationStatus::Failed,
                        _ => ProviderOperationStatus::WaitingExternal,
                    };
                    (mapped, Some(format!("hl:oid:{oid}")))
                }
            };

            Ok(Some(ProviderOperationObservation {
                status,
                provider_ref: Some(provider_ref.to_string()),
                observed_state,
                response: None,
                tx_hash,
                error: None,
            }))
        })
    }
}

/// Step-request schema produced by `market_order_planner::hyperliquid_trade_step`.
/// Parsed inside `HyperliquidProvider::execute_trade` so any planner-side drift
/// shows up as a decoding error rather than silent misrouting.
///
/// Slippage fields (`min_amount_out` / `max_amount_in`) are required on the
/// cross-token path — they bound the Hyperliquid limit price. The hydrator (in
/// `order_executor`) fills `hyperliquid_custody_vault_{id,address}` by role.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyperliquidTradeStepRequest {
    pub order_id: Uuid,
    pub quote_id: Uuid,
    pub order_kind: String,
    pub amount_in: String,
    pub amount_out: String,
    #[serde(default)]
    pub min_amount_out: Option<String>,
    #[serde(default)]
    pub max_amount_in: Option<String>,
    pub input_asset: HyperliquidTradeAssetRef,
    pub output_asset: HyperliquidTradeAssetRef,
    #[serde(default)]
    pub prefund_from_withdrawable: bool,
    #[serde(default)]
    pub hyperliquid_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub hyperliquid_custody_vault_address: Option<String>,
}

/// Step-request schema for a real resting Hyperliquid spot limit order. Unlike
/// `HyperliquidTrade`, this is not a marketable slippage-bounded order: the
/// router derives the HL `limit_px` from `amount_in` and `amount_out`, submits
/// it as GTC, and observes it until filled/cancelled.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyperliquidLimitOrderStepRequest {
    pub order_id: Uuid,
    pub quote_id: Uuid,
    pub amount_in: String,
    pub amount_out: String,
    #[serde(default)]
    pub residual_policy: Option<String>,
    pub input_asset: HyperliquidTradeAssetRef,
    pub output_asset: HyperliquidTradeAssetRef,
    #[serde(default)]
    pub prefund_from_withdrawable: bool,
    #[serde(default)]
    pub hyperliquid_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub hyperliquid_custody_vault_address: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HyperliquidTradeAssetRef {
    pub chain_id: String,
    pub asset: String,
}

/// Step-request schema produced by `market_order_planner::universal_router_swap_step`.
/// The route planner preserves the `priceRoute` returned by Velora so the
/// transaction-build request can send it back exactly, as required by Velora's
/// `/transactions/:network` endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UniversalRouterSwapStepRequest {
    pub order_id: Uuid,
    pub quote_id: Uuid,
    pub order_kind: String,
    pub amount_in: String,
    pub amount_out: String,
    #[serde(default)]
    pub min_amount_out: Option<String>,
    #[serde(default)]
    pub max_amount_in: Option<String>,
    pub input_asset: UniversalRouterAssetRef,
    pub output_asset: UniversalRouterAssetRef,
    pub input_decimals: u8,
    pub output_decimals: u8,
    pub price_route: Value,
    #[serde(default)]
    pub slippage_bps: Option<u64>,
    pub source_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    pub source_custody_vault_address: Option<String>,
    #[serde(default)]
    pub recipient_address: String,
    #[serde(default)]
    pub recipient_custody_vault_id: Option<Uuid>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UniversalRouterAssetRef {
    pub chain_id: String,
    pub asset: String,
}

impl UniversalRouterAssetRef {
    pub fn deposit_asset(&self) -> ProviderResult<DepositAsset> {
        Ok(DepositAsset {
            chain: ChainId::parse(&self.chain_id)
                .map_err(|err| format!("invalid universal router asset chain: {err}"))?,
            asset: AssetId::parse(&self.asset)
                .map_err(|err| format!("invalid universal router asset id: {err}"))?,
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn velora_quote_descriptor(
    request: &ExchangeQuoteRequest,
    network: u64,
    src_token: &str,
    dest_token: &str,
    src_decimals: u8,
    dest_decimals: u8,
    slippage_bps: u64,
    price_route: Value,
    amount_in: &str,
    amount_out: &str,
) -> Value {
    let (order_kind, min_amount_out, max_amount_in) = match &request.order_kind {
        MarketOrderKind::ExactIn { min_amount_out, .. } => {
            ("exact_in", Some(min_amount_out.clone()), None)
        }
        MarketOrderKind::ExactOut { max_amount_in, .. } => {
            ("exact_out", None, Some(max_amount_in.clone()))
        }
    };
    json!({
        "schema_version": 1,
        "kind": "universal_router_swap",
        "order_kind": order_kind,
        "network": network,
        "src_token": src_token,
        "dest_token": dest_token,
        "src_decimals": src_decimals,
        "dest_decimals": dest_decimals,
        "input_asset": {
            "chain_id": request.input_asset.chain.as_str(),
            "asset": request.input_asset.asset.as_str(),
        },
        "output_asset": {
            "chain_id": request.output_asset.chain.as_str(),
            "asset": request.output_asset.asset.as_str(),
        },
        "amount_in": amount_in,
        "amount_out": amount_out,
        "min_amount_out": min_amount_out,
        "max_amount_in": max_amount_in,
        "slippage_bps": slippage_bps,
        "price_route": price_route,
    })
}

fn velora_asset_decimals(asset: &DepositAsset, hinted: Option<u8>) -> Option<u8> {
    match asset.asset {
        AssetId::Native => Some(18),
        AssetId::Reference(_) => hinted,
    }
}

fn velora_token_address(asset: &DepositAsset) -> Option<String> {
    asset.chain.evm_chain_id()?;
    match &asset.asset {
        AssetId::Native => Some(VELORA_NATIVE_TOKEN.to_string()),
        AssetId::Reference(address) => Address::from_str(address)
            .ok()
            .map(|address| format!("{address:#x}")),
    }
}

fn velora_price_route_string(price_route: &Value, field: &'static str) -> ProviderResult<String> {
    velora_value_string(price_route, field)
}

fn velora_response_string(response: &Value, field: &'static str) -> ProviderResult<String> {
    velora_value_string(response, field)
}

fn velora_value_string(value: &Value, field: &'static str) -> ProviderResult<String> {
    match value.get(field) {
        Some(Value::String(raw)) if !raw.trim().is_empty() => Ok(raw.clone()),
        Some(Value::Number(number)) => Ok(number.to_string()),
        Some(other) => Err(format!(
            "velora field {field} was not a string/number: {other}"
        )),
        None => Err(format!("velora response missing field {field}")),
    }
}

fn velora_response_optional_u256(
    response: &Value,
    field: &'static str,
) -> ProviderResult<Option<U256>> {
    match response.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => parse_u256_decimal_or_hex(field, raw).map(Some),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(U256::from)
            .map(Some)
            .ok_or_else(|| format!("velora field {field} does not fit into u64")),
        Some(other) => Err(format!(
            "velora field {field} was not a string/number: {other}"
        )),
    }
}

fn parse_u256_decimal_or_hex(field: &'static str, value: &str) -> ProviderResult<U256> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("velora field {field} must not be empty"));
    }
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        U256::from_str_radix(hex, 16).map_err(|err| format!("invalid hex {field}: {err}"))
    } else {
        U256::from_str_radix(trimmed, 10).map_err(|err| format!("invalid decimal {field}: {err}"))
    }
}

fn encode_erc20_approve(spender: &str, amount: &str) -> ProviderResult<String> {
    let spender = Address::from_str(spender)
        .map_err(|err| format!("invalid velora token approval spender {spender:?}: {err}"))?;
    let amount = parse_u256_decimal_or_hex("approval amount", amount)?;
    let calldata = IERC20Approval::approveCall { spender, amount }.abi_encode();
    Ok(format!("0x{}", hex::encode(calldata)))
}

fn velora_slippage_bps(
    order_kind: &MarketOrderKind,
    amount_in: &str,
    amount_out: &str,
) -> ProviderResult<u64> {
    match order_kind {
        MarketOrderKind::ExactIn { min_amount_out, .. } => {
            let quoted_out = parse_u256_decimal_or_hex("velora amount_out", amount_out)?;
            if quoted_out.is_zero() {
                return Err("velora quote returned zero amount_out".to_string());
            }
            let min_out = parse_u256_decimal_or_hex("velora min_amount_out", min_amount_out)?;
            if min_out >= quoted_out {
                return Ok(0);
            }
            u256_to_capped_bps(
                quoted_out
                    .saturating_sub(min_out)
                    .saturating_mul(U256::from(10_000_u64))
                    / quoted_out,
            )
        }
        MarketOrderKind::ExactOut { max_amount_in, .. } => {
            let quoted_in = parse_u256_decimal_or_hex("velora amount_in", amount_in)?;
            if quoted_in.is_zero() {
                return Err("velora quote returned zero amount_in".to_string());
            }
            let max_in = parse_u256_decimal_or_hex("velora max_amount_in", max_amount_in)?;
            if max_in <= quoted_in {
                return Ok(0);
            }
            u256_to_capped_bps(
                max_in
                    .saturating_sub(quoted_in)
                    .saturating_mul(U256::from(10_000_u64))
                    / quoted_in,
            )
        }
    }
}

fn u256_to_capped_bps(value: U256) -> ProviderResult<u64> {
    if value > U256::from(10_000_u64) {
        return Ok(10_000);
    }
    value
        .to_string()
        .parse::<u64>()
        .map_err(|err| format!("velora slippage bps did not fit u64: {err}"))
}

fn velora_quote_error_is_no_route(body: &str) -> bool {
    let lowered = body.to_ascii_lowercase();
    lowered.contains("no routes")
        || lowered.contains("token not found")
        || lowered.contains("invalid tokens")
        || lowered.contains("bad usd price")
        || lowered.contains("estimated_loss_greater_than_max_impact")
}

fn set_json_value(target: &mut Value, key: &str, value: Value) {
    if let Some(object) = target.as_object_mut() {
        object.insert(key.to_string(), value);
    }
}

fn matches_quote(token: &str) -> bool {
    token == HL_QUOTE_SYMBOL
}

/// Large USDC wire amount used as a practical sentinel `max_amount_in` when
/// the outer request is `exact_out` and the intermediate leg's bound is set
/// downstream rather than at simulation time.
const PRACTICAL_USDC_MAX_WIRE_STR: &str = "1000000000000000";
const HL_BOOK_WALK_ABSOLUTE_EPSILON: f64 = 1e-12;
const HL_BOOK_WALK_RELATIVE_EPSILON: f64 = 1e-12;

/// Simulate filling a marketable slippage-bounded limit order against the
/// current L2 book. Returns the pair of wire amounts `(amount_in, amount_out)`
/// the leg would realise at quote time:
///
/// - `is_buy = true`  walks asks (ascending px) spending USDC for base.
/// - `is_buy = false` walks bids (descending px) selling base for USDC.
///
/// `exact_in` walks forward until the input is exhausted;
/// `exact_out` walks forward until the target output is reached. Both modes
/// enforce the user-supplied slippage bound (`min_amount_out` / `max_amount_in`)
/// against the final totals so a book that isn't deep enough fails quoting.
fn simulate_leg(
    is_buy: bool,
    input_decimals: u8,
    output_decimals: u8,
    base_sz_decimals: u8,
    order_kind: &MarketOrderKind,
    book: &L2BookSnapshot,
) -> ProviderResult<(String, String)> {
    let side: &[L2Level] = if is_buy { book.asks() } else { book.bids() };
    if side.is_empty() {
        return Err("hyperliquid quote: empty book side".to_string());
    }

    match order_kind {
        MarketOrderKind::ExactIn {
            amount_in,
            min_amount_out,
        } => {
            let input_natural = wire_to_f64(amount_in, input_decimals)?;
            let (_, amount_out_natural) =
                walk_book_exact_in(is_buy, input_natural, side, base_sz_decimals)?;
            let min_out_natural = wire_to_f64(min_amount_out, output_decimals)?;
            if amount_out_natural + f64::EPSILON < min_out_natural {
                return Err(format!(
                    "hyperliquid quote: book walk yielded {amount_out_natural} < min_amount_out {min_out_natural}"
                ));
            }
            let amount_out_wire = natural_to_wire(amount_out_natural, output_decimals)?;
            Ok((amount_in.clone(), amount_out_wire))
        }
        MarketOrderKind::ExactOut {
            amount_out,
            max_amount_in,
        } => {
            let output_natural = wire_to_f64(amount_out, output_decimals)?;
            let (amount_in_natural, _) =
                walk_book_exact_out(is_buy, output_natural, side, base_sz_decimals)?;
            let max_in_natural = wire_to_f64(max_amount_in, input_decimals)?;
            if amount_in_natural > max_in_natural + f64::EPSILON {
                return Err(format!(
                    "hyperliquid quote: book walk requires {amount_in_natural} > max_amount_in {max_in_natural}"
                ));
            }
            let amount_in_wire = natural_to_wire(amount_in_natural, input_decimals)?;
            Ok((amount_in_wire, amount_out.clone()))
        }
    }
}

/// Walk a book side spending exactly `input_natural` — USDC when `is_buy`,
/// base otherwise — and return `(input_consumed, output_received)` in
/// natural units. Output on the base side is quantised to `base_sz_decimals`.
fn walk_book_exact_in(
    is_buy: bool,
    input_natural: f64,
    side: &[L2Level],
    base_sz_decimals: u8,
) -> ProviderResult<(f64, f64)> {
    let mut input_remaining = input_natural;
    let mut output_total = 0.0_f64;
    for level in side {
        if input_remaining <= 0.0 {
            break;
        }
        let px = parse_hl_decimal(&level.px, "px")?;
        let sz = parse_hl_decimal(&level.sz, "sz")?;
        if px <= 0.0 || sz <= 0.0 {
            continue;
        }
        let (input_spent, output_gained) = if is_buy {
            // Spending USDC to buy base: base gained = min(sz, usdc_remaining / px).
            let base_from_usdc = input_remaining / px;
            let base_taken = base_from_usdc.min(sz);
            (base_taken * px, base_taken)
        } else {
            // Selling base for USDC: base spent = min(sz, base_remaining).
            let base_taken = input_remaining.min(sz);
            (base_taken, base_taken * px)
        };
        input_remaining -= input_spent;
        output_total += output_gained;
    }
    if input_remaining > book_walk_tolerance(input_natural) {
        return Err(format!(
            "hyperliquid quote: book side could not absorb remaining input {input_remaining}"
        ));
    }
    let output_total = if is_buy {
        // Buying base: quantise the base received to HL's `sz_decimals`.
        round_to_decimals(output_total, usize::from(base_sz_decimals))?
    } else {
        output_total
    };
    Ok((input_natural, output_total))
}

/// Walk a book side until exactly `target_output_natural` is obtained, and
/// return `(input_required, output_received)` in natural units.
fn walk_book_exact_out(
    is_buy: bool,
    target_output_natural: f64,
    side: &[L2Level],
    base_sz_decimals: u8,
) -> ProviderResult<(f64, f64)> {
    let target = if is_buy {
        // Buying base: quantise target to HL's `sz_decimals` upfront so the
        // walk's input maps cleanly to an integer wire amount later.
        round_to_decimals(target_output_natural, usize::from(base_sz_decimals))?
    } else {
        target_output_natural
    };
    let mut output_remaining = target;
    let mut input_total = 0.0_f64;
    for level in side {
        if output_remaining <= 0.0 {
            break;
        }
        let px = parse_hl_decimal(&level.px, "px")?;
        let sz = parse_hl_decimal(&level.sz, "sz")?;
        if px <= 0.0 || sz <= 0.0 {
            continue;
        }
        let (output_gained, input_spent) = if is_buy {
            // Buying base: output is base; each unit of base costs `px` USDC.
            let base_taken = output_remaining.min(sz);
            (base_taken, base_taken * px)
        } else {
            // Selling base for USDC: each unit of base yields `px` USDC.
            let base_for_usdc = output_remaining / px;
            let base_taken = base_for_usdc.min(sz);
            (base_taken * px, base_taken)
        };
        output_remaining -= output_gained;
        input_total += input_spent;
    }
    if output_remaining > book_walk_tolerance(target) {
        return Err(format!(
            "hyperliquid quote: book side could not produce remaining output {output_remaining}"
        ));
    }
    Ok((input_total, target))
}

fn book_walk_tolerance(amount: f64) -> f64 {
    HL_BOOK_WALK_ABSOLUTE_EPSILON.max(amount.abs() * HL_BOOK_WALK_RELATIVE_EPSILON)
}

/// Parse an HL book level string (decimal notation, e.g. `"60000.0"`) into an
/// `f64`. HL never uses scientific notation in `/info` responses.
fn parse_hl_decimal(s: &str, field: &'static str) -> ProviderResult<f64> {
    s.parse::<f64>()
        .map_err(|err| format!("hyperliquid quote: invalid {field} {s:?}: {err}"))
}

/// Convert a natural-unit `f64` into a wire-unit integer string with the
/// given decimal precision. Truncates toward zero at the wire boundary so the
/// result is never larger than what HL's book walk actually realises.
fn natural_to_wire(value: f64, decimals: u8) -> ProviderResult<String> {
    if !value.is_finite() || value < 0.0 {
        return Err(format!(
            "hyperliquid quote: non-finite or negative natural amount {value}"
        ));
    }
    let scaled = value * 10_f64.powi(i32::from(decimals));
    if !scaled.is_finite() {
        return Err(format!(
            "hyperliquid quote: amount {value} overflows wire unit scale"
        ));
    }
    Ok(format!("{}", scaled.trunc() as u128))
}

/// Build the per-leg descriptor the planner reads to emit a
/// `HyperliquidTrade` step. Keys here match `HyperliquidTradeStepRequest`
/// so the planner can copy them through verbatim.
fn hl_leg_descriptor(
    input_asset: &DepositAsset,
    output_asset: &DepositAsset,
    order_kind: &MarketOrderKind,
    amount_in_wire: &str,
    amount_out_wire: &str,
) -> Value {
    let (kind_str, min_amount_out, max_amount_in) = match order_kind {
        // These bounds are consumed by Hyperliquid execution to derive the
        // limit price for this specific leg. Use the simulated leg amounts,
        // not the outer route slippage fields, because the outer fields may
        // be intentionally loose and can otherwise produce invalid HL prices.
        MarketOrderKind::ExactIn { .. } => ("exact_in", Some(amount_out_wire.to_string()), None),
        MarketOrderKind::ExactOut { .. } => ("exact_out", None, Some(amount_in_wire.to_string())),
    };
    json!({
        "order_kind": kind_str,
        "input_asset": {
            "chain_id": input_asset.chain.as_str(),
            "asset": input_asset.asset.as_str(),
        },
        "output_asset": {
            "chain_id": output_asset.chain.as_str(),
            "asset": output_asset.asset.as_str(),
        },
        "amount_in": amount_in_wire,
        "amount_out": amount_out_wire,
        "min_amount_out": min_amount_out,
        "max_amount_in": max_amount_in,
    })
}

fn slippage_bounds_from_request(order_kind: &MarketOrderKind) -> (Option<String>, Option<String>) {
    match order_kind {
        MarketOrderKind::ExactIn { min_amount_out, .. } => (Some(min_amount_out.clone()), None),
        MarketOrderKind::ExactOut { max_amount_in, .. } => (None, Some(max_amount_in.clone())),
    }
}

/// Determine the base token + direction for a cross-token HL leg. One side
/// must be USDC (the quote); the other becomes the base. Errors if neither
/// side is USDC — cross-token legs must pass through USDC.
fn classify_hl_leg(
    input_token: &str,
    input_decimals: u8,
    output_token: &str,
    output_decimals: u8,
) -> ProviderResult<(String, u8, bool)> {
    match (matches_quote(input_token), matches_quote(output_token)) {
        (false, true) => Ok((input_token.to_string(), input_decimals, false)),
        (true, false) => Ok((output_token.to_string(), output_decimals, true)),
        (false, false) => Err(format!(
            "hyperliquid trade: cross-token leg must route through {HL_QUOTE_SYMBOL}; got \
             {input_token} -> {output_token}. The planner emits two legs for non-USDC pairs."
        )),
        (true, true) => {
            Err("hyperliquid trade: USDC -> USDC is not a valid cross-token leg".to_string())
        }
    }
}

/// Compute the HL `sz` (base-token natural-unit amount, decimal string) from
/// the wire-unit amount on whichever side is the base. HL requires `sz`
/// rounded to the base's `sz_decimals`.
fn compute_base_sz(
    is_buy: bool,
    amount_in_wire: &str,
    input_decimals: u8,
    amount_out_wire: &str,
    output_decimals: u8,
    base_sz_decimals: u8,
) -> ProviderResult<String> {
    let (wire, decimals) = if is_buy {
        (amount_out_wire, output_decimals)
    } else {
        (amount_in_wire, input_decimals)
    };
    let natural = wire_to_f64(wire, decimals)?;
    round_to_decimals(natural, usize::from(base_sz_decimals))
        .map(|val| format_decimal_string(val, usize::from(base_sz_decimals)))
}

struct LimitPxInput<'a> {
    is_buy: bool,
    order_kind: &'a str,
    amount_in_wire: &'a str,
    input_decimals: u8,
    amount_out_wire: &'a str,
    output_decimals: u8,
    min_amount_out_wire: Option<&'a str>,
    max_amount_in_wire: Option<&'a str>,
    base_sz_decimals: u8,
}

/// Derive the Hyperliquid `limit_px` that bounds slippage. `limit_px` is expressed as
/// USDC-per-base in natural units; HL spot caps it at 5 significant figures
/// **and** `max(0, HL_SPOT_MAX_DECIMALS - base_sz_decimals)` decimal places.
fn compute_limit_px(input: LimitPxInput<'_>) -> ProviderResult<String> {
    let LimitPxInput {
        is_buy,
        order_kind,
        amount_in_wire,
        input_decimals,
        amount_out_wire,
        output_decimals,
        min_amount_out_wire,
        max_amount_in_wire,
        base_sz_decimals,
    } = input;
    // Identify (usdc_side, base_side) wire amounts for the rate computation.
    // Rate is always USDC-per-base. Use the slippage bound on whichever side
    // the user pinned so the submitted limit order never violates tolerance.
    let (usdc_wire, usdc_decimals, base_wire, base_decimals) = match (is_buy, order_kind) {
        // BUY exact_in: user spends exactly amount_in USDC, wants >= min_amount_out base.
        // Max acceptable rate = amount_in / min_amount_out.
        (true, "exact_in") => {
            let base = min_amount_out_wire.ok_or_else(|| {
                "hyperliquid trade: exact_in buy requires min_amount_out".to_string()
            })?;
            (amount_in_wire, input_decimals, base, output_decimals)
        }
        // BUY exact_out: user gets amount_out base, willing to spend at most max_amount_in USDC.
        // Max acceptable rate = max_amount_in / amount_out.
        (true, "exact_out") => {
            let usdc = max_amount_in_wire.ok_or_else(|| {
                "hyperliquid trade: exact_out buy requires max_amount_in".to_string()
            })?;
            (usdc, input_decimals, amount_out_wire, output_decimals)
        }
        // SELL exact_in: user sells amount_in base, wants >= min_amount_out USDC.
        // Min acceptable rate = min_amount_out / amount_in.
        (false, "exact_in") => {
            let usdc = min_amount_out_wire.ok_or_else(|| {
                "hyperliquid trade: exact_in sell requires min_amount_out".to_string()
            })?;
            (usdc, output_decimals, amount_in_wire, input_decimals)
        }
        // SELL exact_out: user wants amount_out USDC, willing to sell up to max_amount_in base.
        // Min acceptable rate = amount_out / max_amount_in.
        (false, "exact_out") => {
            let base = max_amount_in_wire.ok_or_else(|| {
                "hyperliquid trade: exact_out sell requires max_amount_in".to_string()
            })?;
            (amount_out_wire, output_decimals, base, input_decimals)
        }
        (_, other) => {
            return Err(format!(
                "hyperliquid trade: unknown order_kind {other:?} (expected exact_in | exact_out)"
            ))
        }
    };

    let usdc_natural = wire_to_f64(usdc_wire, usdc_decimals)?;
    let base_natural = wire_to_f64(base_wire, base_decimals)?;
    if base_natural <= 0.0 {
        return Err(format!(
            "hyperliquid trade: non-positive base amount ({base_natural}) computing limit_px"
        ));
    }
    let rate = usdc_natural / base_natural;
    format_hl_spot_price(rate, base_sz_decimals, is_buy)
}

/// Parse a wire-unit amount (integer string) into a natural-unit f64.
/// `decimals` is the number of decimals the asset wire amount carries —
/// UBTC is 8, router-side Hyperliquid USDC is 6, and UETH is 18.
fn wire_to_f64(wire: &str, decimals: u8) -> ProviderResult<f64> {
    let trimmed = wire.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!(
            "hyperliquid trade: wire amount must be a non-negative integer, got {wire:?}"
        ));
    }
    let value: u128 = trimmed
        .parse()
        .map_err(|err| format!("hyperliquid trade: wire amount {wire:?} does not fit: {err}"))?;
    // f64 loses precision past 2^53; wire amounts up to ~9e15 are exact, and
    // HL spot amounts fit comfortably. Loss on the tail is below tick-size.
    Ok(value as f64 / 10_f64.powi(i32::from(decimals)))
}

/// Truncate `value` toward zero to `decimals` decimal places (HL requires
/// `sz` to be quantized to `sz_decimals`).
fn round_to_decimals(value: f64, decimals: usize) -> ProviderResult<f64> {
    if !value.is_finite() || value < 0.0 {
        return Err(format!(
            "hyperliquid trade: non-finite or negative amount {value}"
        ));
    }
    let factor = 10_f64.powi(decimals as i32);
    Ok((value * factor).trunc() / factor)
}

/// Format a positive f64 as a decimal string with at most `decimals` digits
/// after the point, trimming trailing zeros. HL rejects values like "0.00".
fn format_decimal_string(value: f64, decimals: usize) -> String {
    let formatted = format!("{value:.*}", decimals);
    trim_decimal_string(&formatted)
}

fn trim_decimal_string(s: &str) -> String {
    if s.contains('.') {
        let trimmed = s.trim_end_matches('0').trim_end_matches('.');
        if trimmed.is_empty() {
            "0".to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        s.to_string()
    }
}

/// Format an HL spot price: at most 5 significant figures AND at most
/// `HL_SPOT_MAX_DECIMALS - base_sz_decimals` decimal places. Rounds down for
/// buys (limit_px ≤ user max) and up for sells (limit_px ≥ user min) so the
/// submitted limit order never violates the user's slippage bound.
fn format_hl_spot_price(rate: f64, base_sz_decimals: u8, is_buy: bool) -> ProviderResult<String> {
    if !rate.is_finite() || rate <= 0.0 {
        return Err(format!(
            "hyperliquid trade: non-positive or non-finite rate {rate}"
        ));
    }
    let max_decimals =
        (HL_SPOT_MAX_DECIMALS.saturating_sub(i32::from(base_sz_decimals))).max(0) as usize;
    let magnitude = rate.log10().floor() as i32;
    let sig_fig_decimals = (HL_SPOT_PRICE_SIG_FIGS - 1 - magnitude).max(0) as usize;
    let allowed_decimals = sig_fig_decimals.min(max_decimals);
    let factor = 10_f64.powi(allowed_decimals as i32);
    let quantized = if is_buy {
        (rate * factor).floor() / factor
    } else {
        (rate * factor).ceil() / factor
    };
    if quantized <= 0.0 {
        return Err(format!(
            "hyperliquid trade: rate {rate} quantized to zero at {allowed_decimals} decimals"
        ));
    }
    Ok(format_decimal_string(quantized, allowed_decimals))
}

/// Build the `ExchangeQuote` for a same-token no-op: HL-side amounts match
/// 1:1 with the requested side, carrying the user-supplied slippage bound
/// through unchanged so `validate_provider_quote` has something to check.
fn hyperliquid_no_op_quote(
    request: &ExchangeQuoteRequest,
    provider_id: &str,
    token: &str,
) -> ExchangeQuote {
    let (amount_in, amount_out, min_amount_out, max_amount_in) = match &request.order_kind {
        MarketOrderKind::ExactIn {
            amount_in,
            min_amount_out,
        } => (
            amount_in.clone(),
            amount_in.clone(),
            Some(min_amount_out.clone()),
            None,
        ),
        MarketOrderKind::ExactOut {
            amount_out,
            max_amount_in,
        } => (
            amount_out.clone(),
            amount_out.clone(),
            None,
            Some(max_amount_in.clone()),
        ),
    };
    ExchangeQuote {
        provider_id: provider_id.to_string(),
        amount_in,
        amount_out,
        min_amount_out,
        max_amount_in,
        provider_quote: json!({
            "schema_version": 1,
            "kind": "spot_no_op",
            "token": token,
        }),
        expires_at: Utc::now() + chrono::Duration::minutes(10),
    }
}

fn normalize_base_url(base_url: impl Into<String>) -> String {
    base_url.into().trim_end_matches('/').to_string()
}

fn hyperliquid_api_network(network: HyperliquidCallNetwork) -> HyperliquidApiNetwork {
    match network {
        HyperliquidCallNetwork::Mainnet => HyperliquidApiNetwork::Mainnet,
        HyperliquidCallNetwork::Testnet => HyperliquidApiNetwork::Testnet,
    }
}

#[must_use]
pub fn ethereum_native_asset() -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse("evm:1").expect("hardcoded ethereum chain id must be valid"),
        asset: AssetId::Native,
    }
}

#[cfg(test)]
mod hyperliquid_math_tests {
    use super::{
        cctp_quote_amounts, classify_hl_leg, compute_base_sz, compute_limit_px, decimal_bps_string,
        encode_erc20_approve, format_hl_spot_price, hl_leg_descriptor,
        observed_unit_operation_fingerprints, parse_decimal_bps_to_micros,
        parse_decimal_to_raw_units_floor, seen_operation_fingerprints_from_request,
        unit_operation_provider_status, unit_withdrawal_minimum_raw, velora_slippage_bps,
        wire_to_f64, BridgeProvider, BridgeQuoteRequest, CctpQuoteAmounts,
        HyperliquidBridgeProvider, HyperliquidCallNetwork, LimitPxInput,
        HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW,
    };
    use crate::{
        models::{MarketOrderKind, ProviderOperationStatus},
        protocol::{AssetId, ChainId, DepositAsset},
        services::AssetRegistry,
    };
    use alloy::primitives::U256;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn classify_hl_leg_sell_base_for_usdc() {
        let (base, base_decimals, is_buy) = classify_hl_leg("UBTC", 8, "USDC", 8).unwrap();
        assert_eq!(base, "UBTC");
        assert_eq!(base_decimals, 8);
        assert!(!is_buy);
    }

    #[test]
    fn classify_hl_leg_buy_base_with_usdc() {
        let (base, base_decimals, is_buy) = classify_hl_leg("USDC", 8, "UETH", 18).unwrap();
        assert_eq!(base, "UETH");
        assert_eq!(base_decimals, 18);
        assert!(is_buy);
    }

    #[test]
    fn classify_hl_leg_rejects_non_usdc_pair() {
        assert!(classify_hl_leg("UBTC", 8, "UETH", 18).is_err());
    }

    #[test]
    fn classify_hl_leg_rejects_usdc_to_usdc() {
        assert!(classify_hl_leg("USDC", 8, "USDC", 8).is_err());
    }

    #[test]
    fn velora_slippage_bps_exact_in_uses_requested_min_output() {
        let order_kind = MarketOrderKind::ExactIn {
            amount_in: "1000".to_string(),
            min_amount_out: "950".to_string(),
        };

        assert_eq!(
            velora_slippage_bps(&order_kind, "1000", "1000").unwrap(),
            500
        );
    }

    #[test]
    fn velora_slippage_bps_exact_out_uses_requested_max_input() {
        let order_kind = MarketOrderKind::ExactOut {
            amount_out: "1000".to_string(),
            max_amount_in: "1100".to_string(),
        };

        assert_eq!(
            velora_slippage_bps(&order_kind, "1000", "1000").unwrap(),
            1000
        );
    }

    #[test]
    fn velora_approval_calldata_targets_erc20_approve() {
        let calldata =
            encode_erc20_approve("0x9999999999999999999999999999999999999999", "12345").unwrap();

        assert!(calldata.starts_with("0x095ea7b3"));
        assert!(calldata.contains("9999999999999999999999999999999999999999"));
    }

    #[test]
    fn cctp_decimal_fee_parser_preserves_fractional_bps() {
        let fee = parse_decimal_bps_to_micros("1.3").unwrap();
        assert_eq!(fee, 1_300_000);
        assert_eq!(decimal_bps_string(fee), "1.3");
    }

    #[test]
    fn cctp_exact_in_quote_deducts_circle_fee() {
        let order_kind = MarketOrderKind::ExactIn {
            amount_in: "1000000".to_string(),
            min_amount_out: "1".to_string(),
        };

        assert_eq!(
            cctp_quote_amounts(&order_kind, 1_300_000).unwrap(),
            Some(CctpQuoteAmounts {
                amount_in: U256::from(1_000_000_u64),
                amount_out: U256::from(999_870_u64),
                max_fee: U256::from(130_u64),
            })
        );
    }

    #[test]
    fn cctp_exact_out_quote_grosses_up_circle_fee() {
        let order_kind = MarketOrderKind::ExactOut {
            amount_out: "999870".to_string(),
            max_amount_in: "1000000".to_string(),
        };

        assert_eq!(
            cctp_quote_amounts(&order_kind, 1_300_000).unwrap(),
            Some(CctpQuoteAmounts {
                amount_in: U256::from(1_000_000_u64),
                amount_out: U256::from(999_870_u64),
                max_fee: U256::from(130_u64),
            })
        );
    }

    #[test]
    fn wire_to_f64_converts_ubtc_wire() {
        let value = wire_to_f64("100000000", 8).unwrap();
        assert!((value - 1.0).abs() < 1e-12);
    }

    #[test]
    fn wire_to_f64_rejects_non_integer() {
        assert!(wire_to_f64("1.5", 8).is_err());
        assert!(wire_to_f64("", 8).is_err());
        assert!(wire_to_f64("-1", 8).is_err());
    }

    #[test]
    fn parse_decimal_to_raw_units_floor_truncates_hyperliquid_fee_dust() {
        let raw = parse_decimal_to_raw_units_floor("0.0007594681", 8).unwrap();
        assert_eq!(raw.to_string(), "75946");
    }

    #[test]
    fn unit_withdrawal_minimum_raw_returns_btc_sats_minimum() {
        let btc = DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        };
        assert_eq!(unit_withdrawal_minimum_raw(&btc).to_string(), "30000");
    }

    #[tokio::test]
    async fn hyperliquid_bridge_withdrawal_quote_deducts_fee_exact_in() {
        let provider = HyperliquidBridgeProvider::new(
            "http://localhost:1",
            HyperliquidCallNetwork::Testnet,
            Arc::new(AssetRegistry::default()),
        );
        let quote = provider
            .quote_bridge(BridgeQuoteRequest {
                source_asset: DepositAsset {
                    chain: ChainId::parse("hyperliquid").unwrap(),
                    asset: AssetId::Native,
                },
                destination_asset: DepositAsset {
                    chain: ChainId::parse("evm:42161").unwrap(),
                    asset: AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
                },
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "101000000".to_string(),
                    min_amount_out: "1".to_string(),
                },
                recipient_address: "0x0000000000000000000000000000000000000001".to_string(),
                depositor_address: "0x0000000000000000000000000000000000000002".to_string(),
                partial_fills_enabled: false,
            })
            .await
            .unwrap()
            .expect("quote");

        assert_eq!(
            HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW, 1_000_000,
            "test documents the current HL USDC withdrawal fee model"
        );
        assert_eq!(quote.amount_in, "101000000");
        assert_eq!(quote.amount_out, "100000000");
        assert_eq!(
            quote
                .provider_quote
                .get("kind")
                .and_then(|value| value.as_str()),
            Some("hyperliquid_bridge_withdrawal")
        );
    }

    #[tokio::test]
    async fn hyperliquid_bridge_withdrawal_quote_grosses_up_exact_out() {
        let provider = HyperliquidBridgeProvider::new(
            "http://localhost:1",
            HyperliquidCallNetwork::Testnet,
            Arc::new(AssetRegistry::default()),
        );
        let quote = provider
            .quote_bridge(BridgeQuoteRequest {
                source_asset: DepositAsset {
                    chain: ChainId::parse("hyperliquid").unwrap(),
                    asset: AssetId::Native,
                },
                destination_asset: DepositAsset {
                    chain: ChainId::parse("evm:42161").unwrap(),
                    asset: AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
                },
                order_kind: MarketOrderKind::ExactOut {
                    amount_out: "100000000".to_string(),
                    max_amount_in: U256::MAX.to_string(),
                },
                recipient_address: "0x0000000000000000000000000000000000000001".to_string(),
                depositor_address: "0x0000000000000000000000000000000000000002".to_string(),
                partial_fills_enabled: false,
            })
            .await
            .unwrap()
            .expect("quote");

        assert_eq!(quote.amount_in, "101000000");
        assert_eq!(quote.amount_out, "100000000");
    }

    #[test]
    fn hl_leg_descriptor_uses_quoted_exact_in_output_as_execution_bound() {
        let usdc = DepositAsset {
            chain: ChainId::parse("hyperliquid").unwrap(),
            asset: AssetId::Native,
        };
        let btc = DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        };
        let leg = hl_leg_descriptor(
            &usdc,
            &btc,
            &MarketOrderKind::ExactIn {
                amount_in: "59984517".to_string(),
                min_amount_out: "1".to_string(),
            },
            "59984517",
            "77000",
        );

        assert_eq!(leg["order_kind"], json!("exact_in"));
        assert_eq!(leg["amount_out"], json!("77000"));
        assert_eq!(leg["min_amount_out"], json!("77000"));
    }

    #[test]
    fn hl_leg_descriptor_uses_quoted_exact_out_input_as_execution_bound() {
        let usdc = DepositAsset {
            chain: ChainId::parse("hyperliquid").unwrap(),
            asset: AssetId::Native,
        };
        let btc = DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        };
        let leg = hl_leg_descriptor(
            &usdc,
            &btc,
            &MarketOrderKind::ExactOut {
                amount_out: "77000".to_string(),
                max_amount_in: "100000000".to_string(),
            },
            "59984517",
            "77000",
        );

        assert_eq!(leg["order_kind"], json!("exact_out"));
        assert_eq!(leg["amount_in"], json!("59984517"));
        assert_eq!(leg["max_amount_in"], json!("59984517"));
    }

    #[test]
    fn compute_base_sz_sell_uses_amount_in_truncated() {
        // amount_in = 0.12345678 UBTC wire, sz_decimals = 5 → "0.12345"
        let sz = compute_base_sz(false, "12345678", 8, "6000000000", 8, 5).unwrap();
        assert_eq!(sz, "0.12345");
    }

    #[test]
    fn compute_base_sz_buy_uses_amount_out_truncated() {
        // amount_out = 1.50000009 UBTC wire (decimals 8), sz_decimals = 5 → "1.5"
        let sz = compute_base_sz(true, "75000000000", 8, "150000009", 8, 5).unwrap();
        assert_eq!(sz, "1.5");
    }

    #[test]
    fn compute_limit_px_exact_in_sell_uses_min_amount_out() {
        // sell 1 UBTC for at least 50000 USDC → rate = 50000 → "50000"
        let px = compute_limit_px(LimitPxInput {
            is_buy: false,
            order_kind: "exact_in",
            amount_in_wire: "100000000",
            input_decimals: 8,
            amount_out_wire: "5000000000000",
            output_decimals: 8,
            min_amount_out_wire: Some("5000000000000"),
            max_amount_in_wire: None,
            base_sz_decimals: 5,
        })
        .unwrap();
        assert_eq!(px, "50000");
    }

    #[test]
    fn compute_limit_px_exact_in_buy_uses_min_amount_out() {
        // buy with 100000 USDC, need at least 1.5 UBTC → rate = 66666.66... → floor to 5 sig figs → "66666"
        let px = compute_limit_px(LimitPxInput {
            is_buy: true,
            order_kind: "exact_in",
            amount_in_wire: "10000000000000",
            input_decimals: 8,
            amount_out_wire: "150000000",
            output_decimals: 8,
            min_amount_out_wire: Some("150000000"),
            max_amount_in_wire: None,
            base_sz_decimals: 5,
        })
        .unwrap();
        assert_eq!(px, "66666");
    }

    #[test]
    fn compute_limit_px_exact_out_buy_uses_max_amount_in() {
        // buy 1 UBTC, willing to spend up to 70000 USDC → rate = 70000 → "70000"
        let px = compute_limit_px(LimitPxInput {
            is_buy: true,
            order_kind: "exact_out",
            amount_in_wire: "7000000000000",
            input_decimals: 8,
            amount_out_wire: "100000000",
            output_decimals: 8,
            min_amount_out_wire: None,
            max_amount_in_wire: Some("7000000000000"),
            base_sz_decimals: 5,
        })
        .unwrap();
        assert_eq!(px, "70000");
    }

    #[test]
    fn compute_limit_px_exact_out_sell_uses_max_amount_in() {
        // sell up to 1.1 UBTC for 50000 USDC → rate = 45454.54... → ceil to 5 sig figs → "45455"
        let px = compute_limit_px(LimitPxInput {
            is_buy: false,
            order_kind: "exact_out",
            amount_in_wire: "110000000",
            input_decimals: 8,
            amount_out_wire: "5000000000000",
            output_decimals: 8,
            min_amount_out_wire: None,
            max_amount_in_wire: Some("110000000"),
            base_sz_decimals: 5,
        })
        .unwrap();
        assert_eq!(px, "45455");
    }

    #[test]
    fn format_hl_spot_price_ueth_pair_rounds_to_decimals() {
        // UETH sz_decimals = 4 → max 4 decimals, rate 3000 magnitude 3 → 1 decimal allowed → "3000"
        let px = format_hl_spot_price(3000.0, 4, true).unwrap();
        assert_eq!(px, "3000");
    }

    #[test]
    fn format_hl_spot_price_small_rate_respects_sig_figs() {
        // rate 0.00012345 at base_sz_decimals = 0 → max_decimals = 8, sig_fig_decimals = 8
        // floor buy preserves all 5 sig figs → "0.00012345"
        let px = format_hl_spot_price(0.00012345, 0, true).unwrap();
        assert_eq!(px, "0.00012345");
    }

    #[test]
    fn format_hl_spot_price_rejects_non_positive() {
        assert!(format_hl_spot_price(0.0, 5, true).is_err());
        assert!(format_hl_spot_price(-1.0, 5, true).is_err());
        assert!(format_hl_spot_price(f64::NAN, 5, true).is_err());
    }

    #[test]
    fn format_hl_spot_price_sell_rounds_up() {
        // rate 3000.55 at sz_decimals = 5 → max 3 decimals, but 5 sig figs caps at 1 decimal
        // ceil sell → 3000.6
        let px = format_hl_spot_price(3000.55, 5, false).unwrap();
        assert_eq!(px, "3000.6");
    }

    #[test]
    fn format_hl_spot_price_buy_rounds_down() {
        // rate 3000.55 at sz_decimals = 5 → 1 decimal allowed, floor buy → 3000.5
        let px = format_hl_spot_price(3000.55, 5, true).unwrap();
        assert_eq!(px, "3000.5");
    }

    #[test]
    fn observed_unit_operation_fingerprints_only_collect_matching_protocol_address() {
        let matching = hyperunit_client::UnitOperation {
            operation_id: Some("op-1".to_string()),
            op_created_at: Some("2026-04-22T13:30:24.436405Z".to_string()),
            protocol_address: Some("0xproto".to_string()),
            source_address: None,
            destination_address: None,
            source_chain: None,
            destination_chain: None,
            source_amount: None,
            destination_fee_amount: None,
            sweep_fee_amount: None,
            state: Some("done".to_string()),
            source_tx_hash: Some("0xabc:0".to_string()),
            destination_tx_hash: Some("0xdef".to_string()),
            source_tx_confirmations: None,
            destination_tx_confirmations: None,
            position_in_withdraw_queue: None,
            broadcast_at: None,
            asset: None,
            state_started_at: None,
            state_updated_at: None,
            state_next_attempt_at: None,
        };
        let other = hyperunit_client::UnitOperation {
            protocol_address: Some("0xother".to_string()),
            ..matching.clone()
        };

        let fingerprints = observed_unit_operation_fingerprints(&[matching], &[other], "0xproto");

        assert!(fingerprints.contains("operationId:op-1"));
        assert!(fingerprints.contains("sourceTxHash:0xabc:0"));
        assert!(fingerprints.contains("destinationTxHash:0xdef"));
        assert!(!fingerprints.contains("operationId:op-1-foreign"));
    }

    #[test]
    fn unit_operation_status_completes_queued_with_destination_tx_hash() {
        let op = hyperunit_client::UnitOperation {
            state: Some("queuedForWithdraw".to_string()),
            destination_tx_hash: Some(
                "8c557f2daa08c2761f64d0abbd8cc6e9635507a3ebb2aed0c70dbb40821f183e".to_string(),
            ),
            ..empty_unit_operation()
        };

        assert_eq!(
            unit_operation_provider_status(&op),
            ProviderOperationStatus::Completed
        );
    }

    #[test]
    fn unit_operation_status_waits_for_queued_without_destination_tx_hash() {
        let op = hyperunit_client::UnitOperation {
            state: Some("queuedForWithdraw".to_string()),
            destination_tx_hash: Some(" ".to_string()),
            ..empty_unit_operation()
        };

        assert_eq!(
            unit_operation_provider_status(&op),
            ProviderOperationStatus::WaitingExternal
        );
    }

    #[test]
    fn seen_operation_fingerprints_from_request_reads_array() {
        let request = json!({
            "seen_operation_fingerprints": [
                "operationId:old-op",
                "sourceTxHash:0xabc:0"
            ]
        });

        let fingerprints = seen_operation_fingerprints_from_request(&request);

        assert!(fingerprints.contains("operationId:old-op"));
        assert!(fingerprints.contains("sourceTxHash:0xabc:0"));
    }

    fn empty_unit_operation() -> hyperunit_client::UnitOperation {
        hyperunit_client::UnitOperation {
            operation_id: None,
            op_created_at: None,
            protocol_address: None,
            source_address: None,
            destination_address: None,
            source_chain: None,
            destination_chain: None,
            source_amount: None,
            destination_fee_amount: None,
            sweep_fee_amount: None,
            state: None,
            source_tx_hash: None,
            destination_tx_hash: None,
            source_tx_confirmations: None,
            destination_tx_confirmations: None,
            position_in_withdraw_queue: None,
            broadcast_at: None,
            asset: None,
            state_started_at: None,
            state_updated_at: None,
            state_next_attempt_at: None,
        }
    }
}
