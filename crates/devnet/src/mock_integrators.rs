use alloy::{
    primitives::{Address, Bytes, FixedBytes, TxHash, U256},
    providers::{Provider, ProviderBuilder},
    sol,
    sol_types::SolCall,
};
use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use hyperliquid_client::{UserFill, UserFunding, UserNonFundingLedgerUpdate};
use hyperunit_client::{UnitAsset, UnitChain, UnitOperation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};

use crate::{
    manifest::{SALT_MOCK_ACROSS, SALT_MOCK_HYPERLIQUID_BRIDGE, SALT_MOCK_HYPERUNIT},
    MultichainAccount,
};
use eip7702_paymaster::{Execution, PaymasterError, PaymasterHandle};

mod across;
mod chainalysis;
mod cctp;
mod coinbase;
mod hyperliquid;
mod hyperunit;
mod velora;

pub use across::{MockAcrossChainConfig, MockAcrossDepositRecord};
pub use chainalysis::{MockAddressRiskLevel, MockAddressScreeningRule};
pub use cctp::{MockCctpBurnRecord, MockCctpChainConfig, MockCctpDelayReason};
pub use hyperunit::{
    MockUnitGenerateAddressRequest, MockUnitOperationKind, MockUnitOperationRecord,
};
use across::{
    maybe_spawn_across_deposit_indexer, mock_across_deposit_status, mock_across_real_swap_approval,
    AcrossMockState, ResolvedMockAcrossChainConfig,
};
use chainalysis::{
    mock_chainalysis_address_risk, normalize_mock_screening_address, ChainalysisMockState,
};
use cctp::{
    cctp_domain_for_chain_id, maybe_spawn_cctp_burn_indexer, mock_cctp_burn_fees,
    mock_cctp_messages, CctpMockState,
};
use coinbase::{mock_coinbase_spot_price, CoinbaseMockState};
use hyperliquid::{
    maybe_spawn_hyperliquid_bridge_indexer, mock_hyperliquid_exchange, mock_hyperliquid_info,
    HyperliquidVenueState,
};
use crate::hyperliquid_core::HyperliquidCore;
use hyperunit::{
    complete_mock_unit_operation, complete_mock_unit_operation_with_observation,
    complete_unit_withdrawal_after_hyperliquid_transfer, fail_mock_unit_operation,
    maybe_spawn_unit_deposit_indexers, mock_unit_gen, mock_unit_operations, snapshot_unit_operations,
    MockUnitOperationEntry, UnitMockState, UnitOperationObservation,
};
use velora::{
    default_velora_usd_prices, mock_velora_prices, mock_velora_transaction, VeloraMockState,
};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
        function balanceOf(address account) external view returns (uint256);
        function transfer(address recipient, uint256 amount) external returns (bool);

        event Transfer(address indexed from, address indexed to, uint256 value);
    }
}

sol! {
    interface IMockMintableERC20 {
        function mint(address recipient, uint256 amount) external;
    }
}

// Anvil block-mining waits per HU op are pure mock artificial latency. 1 means
// "single block of finality" — enough that the receipt watcher sees the tx, no
// fake delay. (Was 10, adding ~20s of pure waiting per order at 10k scale.)
pub(crate) const MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS: u64 = 1;
const DEFAULT_MOCK_UNIT_WITHDRAWAL_RELEASE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MockService {
    Hyperunit,
    HyperliquidBridge,
    Across,
}

impl MockService {
    pub const ALL: [Self; 3] = [Self::Hyperunit, Self::HyperliquidBridge, Self::Across];

    pub fn salt(self) -> u32 {
        match self {
            Self::Hyperunit => SALT_MOCK_HYPERUNIT,
            Self::HyperliquidBridge => SALT_MOCK_HYPERLIQUID_BRIDGE,
            Self::Across => SALT_MOCK_ACROSS,
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Hyperunit => "HyperUnit",
            Self::HyperliquidBridge => "Hyperliquid Bridge",
            Self::Across => "Across",
        }
    }

    pub fn metric_label(self, chain_id: u64) -> &'static str {
        match (self, chain_id) {
            (Self::Hyperunit, 1) => "mock_hyperunit_eth",
            (Self::Hyperunit, 8453) => "mock_hyperunit_base",
            (Self::Hyperunit, 42161) => "mock_hyperunit_arb",
            (Self::Hyperunit, _) => "mock_hyperunit_dev",
            (Self::HyperliquidBridge, 1) => "mock_hyperliquid_bridge_eth",
            (Self::HyperliquidBridge, 8453) => "mock_hyperliquid_bridge_base",
            (Self::HyperliquidBridge, 42161) => "mock_hyperliquid_bridge_arb",
            (Self::HyperliquidBridge, _) => "mock_hyperliquid_bridge_dev",
            (Self::Across, 1) => "mock_across_eth",
            (Self::Across, 8453) => "mock_across_base",
            (Self::Across, 42161) => "mock_across_arb",
            (Self::Across, _) => "mock_across_dev",
        }
    }
}

#[derive(Clone)]
pub struct MockServicePaymaster {
    service: MockService,
    chain_id: u64,
    handle: Option<PaymasterHandle>,
    #[cfg(test)]
    test_behavior: Option<MockServicePaymasterTestBehavior>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum MockServicePaymasterTestBehavior {
    Hang(Duration),
    Succeed(TxHash),
}

impl fmt::Debug for MockServicePaymaster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("MockServicePaymaster");
        debug
            .field("service", &self.service)
            .field("chain_id", &self.chain_id)
            .field("handle", &self.handle.is_some());
        #[cfg(test)]
        debug.field("test_behavior", &self.test_behavior);
        debug.finish()
    }
}

impl MockServicePaymaster {
    pub fn live(service: MockService, chain_id: u64, handle: PaymasterHandle) -> Self {
        Self {
            service,
            chain_id,
            handle: Some(handle),
            #[cfg(test)]
            test_behavior: None,
        }
    }

    #[cfg(test)]
    fn stub(service: MockService, chain_id: u64) -> Self {
        Self {
            service,
            chain_id,
            handle: None,
            test_behavior: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn hanging(service: MockService, chain_id: u64, duration: Duration) -> Self {
        Self {
            service,
            chain_id,
            handle: None,
            test_behavior: Some(MockServicePaymasterTestBehavior::Hang(duration)),
        }
    }

    #[cfg(test)]
    pub(crate) fn successful(service: MockService, chain_id: u64, tx_hash: TxHash) -> Self {
        Self {
            service,
            chain_id,
            handle: None,
            test_behavior: Some(MockServicePaymasterTestBehavior::Succeed(tx_hash)),
        }
    }

    pub fn service(&self) -> MockService {
        self.service
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub async fn submit(&self, execution: Execution) -> eip7702_paymaster::Result<TxHash> {
        #[cfg(test)]
        if let Some(test_behavior) = self.test_behavior {
            return match test_behavior {
                MockServicePaymasterTestBehavior::Hang(duration) => {
                    tokio::time::sleep(duration).await;
                    Err(PaymasterError::Actor {
                        message: "test paymaster hang completed without submitting".to_string(),
                    })
                }
                MockServicePaymasterTestBehavior::Succeed(tx_hash) => Ok(tx_hash),
            };
        }

        self.handle
            .as_ref()
            .ok_or_else(|| PaymasterError::Actor {
                message: format!(
                    "mock {} paymaster for chain {} is not initialized",
                    self.service.display_name(),
                    self.chain_id
                ),
            })?
            .submit(execution)
            .await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MockAssetRef {
    pub chain_id: String,
    pub asset: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockProviderQuoteResponse {
    pub provider_id: String,
    pub amount_in: String,
    pub amount_out: String,
    pub min_amount_out: Option<String>,
    pub max_amount_in: Option<String>,
    pub provider_quote: Value,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockIntegratorErrorResponse {
    pub error: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockIntegratorLedgerSnapshot {
    pub bridged_balances: BTreeMap<String, String>,
    pub hyperliquid_balances: BTreeMap<String, String>,
    pub recipient_balances: BTreeMap<String, String>,
    pub unit_operations: Vec<MockUnitOperationRecord>,
    pub cctp_burns: Vec<MockCctpBurnRecord>,
    pub hyperliquid_exchange_submissions: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct MockIntegratorConfig {
    /// TCP address the mock HTTP server should bind.
    pub bind_addr: SocketAddr,
    /// Optional URL used in the returned server handle instead of deriving one
    /// from the bound socket. This is useful when binding 0.0.0.0 but
    /// advertising localhost or a Docker Compose service name.
    pub advertised_base_url: Option<String>,
    /// Address of the mock SpokePool contract deployed to the test Anvil.
    /// When set, the mock GET `/swap/approval` handler encodes a real
    /// `deposit(...)` call targeting this address (matching real Across
    /// semantics: production submits the returned `swapTx` on-chain).
    pub across_spoke_pool_address: Option<String>,
    /// RPC URL (ws:// or http://) of the chain hosting the mock SpokePool.
    /// When set alongside `across_spoke_pool_address`, a background indexer
    /// polls `FundsDeposited` logs and serves them via `GET /deposit/status`
    /// in the same shape as the real Across API.
    pub across_evm_rpc_url: Option<String>,
    /// Per-origin-chain Across mock configuration. The real Across API routes
    /// approval simulation and deposit indexing by origin chain; the mock must
    /// do the same once local tests span multiple EVM chains.
    pub across_chains: BTreeMap<u64, MockAcrossChainConfig>,
    /// Whether Hyperliquid L1 actions must be signed under the mainnet
    /// source-byte ("a") vs testnet ("b"). Defaults to testnet so tests
    /// default to the non-mainnet signing path the router uses in devnet.
    pub mainnet_hyperliquid: bool,
    /// Optional bearer token the Across mock requires on `/swap/approval` and
    /// `/deposit/status`. When unset, auth is not enforced.
    pub across_api_key: Option<String>,
    /// Optional integrator id the Across mock requires in `/swap/approval`.
    /// When unset, any non-empty or absent integrator id is accepted.
    pub across_integrator_id: Option<String>,
    /// Address of the mock Hyperliquid Bridge2 contract deployed to an EVM
    /// test chain. Native Hyperliquid USDC deposits are modeled as ERC20
    /// transfers into this address.
    pub hyperliquid_bridge_address: Option<String>,
    /// RPC URL (ws:// or http://) of the chain hosting the mock Hyperliquid
    /// bridge and its USDC token. When set with `hyperliquid_bridge_address`
    /// and `hyperliquid_usdc_token_address`, a background indexer watches
    /// token `Transfer` logs into the bridge and credits the sender's mock
    /// Hyperliquid USDC balance.
    pub hyperliquid_evm_rpc_url: Option<String>,
    /// ERC20 token address used as "native Arbitrum USDC" for the mock
    /// Hyperliquid bridge.
    pub hyperliquid_usdc_token_address: Option<String>,
    /// Artificial delay between observing a native Hyperliquid USDC bridge
    /// deposit on the EVM chain and crediting the mock Hyperliquid
    /// clearinghouse ledger.
    pub hyperliquid_bridge_deposit_latency: Duration,
    /// Deterministic probability that an otherwise valid native Hyperliquid
    /// USDC bridge deposit never credits the mock clearinghouse ledger, in
    /// basis points.
    pub hyperliquid_bridge_deposit_failure_probability_bps: u16,
    /// EVM RPC URLs used to detect native Unit deposits into generated
    /// protocol addresses.
    pub unit_evm_rpc_urls: BTreeMap<String, String>,
    /// Hard cap for mock Unit withdrawal release work after Hyperliquid
    /// accepts the source transfer.
    pub unit_withdrawal_release_timeout: Duration,
    /// EVM RPC URLs where every mock service treasury should be funded,
    /// delegated, and backed by its own paymaster.
    pub mock_service_evm_chains: BTreeMap<u64, String>,
    /// Pre-spawned mock-service paymasters keyed by `(service, chain_id)`.
    pub mock_service_paymasters: BTreeMap<(MockService, u64), MockServicePaymaster>,
    /// Bitcoin Core RPC URL and cookie used to detect mempool/confirmed Unit
    /// deposits into generated protocol addresses.
    pub unit_bitcoin_rpc_url: Option<String>,
    pub unit_bitcoin_cookie_path: Option<PathBuf>,
    /// Address of the mock CCTP TokenMessengerV2 contract deployed to the
    /// source EVM test chain. When set with `cctp_evm_rpc_url`, the mock Iris
    /// endpoint indexes burn events from this contract.
    pub cctp_token_messenger_address: Option<String>,
    /// RPC URL of the source chain hosting the mock CCTP TokenMessengerV2.
    pub cctp_evm_rpc_url: Option<String>,
    /// Token address that mock CCTP receiveMessage should mint on the
    /// destination chain.
    pub cctp_destination_token_address: Option<String>,
    /// Per-source-chain CCTP mock configuration. Real Iris indexes burns by
    /// source domain, so local multi-chain tests need one indexer per source
    /// EVM chain.
    pub cctp_chains: BTreeMap<u64, MockCctpChainConfig>,
    /// Destination token address keyed by CCTP destination domain.
    pub cctp_destination_token_addresses: BTreeMap<u32, String>,
    /// Artificial delay between indexing a burn and returning a completed Iris
    /// attestation.
    pub cctp_attestation_latency: Duration,
    /// When set, every indexed CCTP burn returns an Iris response carrying
    /// this `delayReason`. `AmountAboveMax` is terminal (router treats it as
    /// `Failed`); the others stay `Pending` per Circle's spec.
    pub cctp_attestation_delay_reason: Option<MockCctpDelayReason>,
    /// Chainalysis-compatible local address-screening responses keyed by
    /// normalized address. Addresses absent from this map default to Low risk.
    pub address_screening_rules: BTreeMap<String, MockAddressScreeningRule>,
    /// Per-network deployed MockVeloraSwap contract addresses. The mock Velora
    /// quote/transaction path requires this so execution always spends input and
    /// creates output on-chain.
    pub velora_swap_contract_addresses: BTreeMap<u64, String>,
    /// Number of upcoming Velora transaction builds that should return a
    /// temporary 503 error in the same JSON error envelope shape Velora documents.
    pub velora_transaction_fail_next_n: usize,
    /// Number of upcoming Velora transaction builds that should reject the
    /// submitted priceRoute as stale using Velora's non-2xx JSON error envelope.
    pub velora_transaction_stale_quote_fail_next_n: usize,
    /// Artificial delay before the mock Bridge2 releases the net `withdraw3`
    /// payout onto the EVM chain. Defaults to zero.
    pub hyperliquid_withdrawal_latency: Duration,
    /// Deterministic base fee applied to mock Across quotes, in basis points.
    pub across_quote_fee_bps: u16,
    /// Deterministic per-quote jitter added on top of `across_quote_fee_bps`,
    /// in basis points. The actual jitter is derived from the request.
    pub across_quote_jitter_bps: u16,
    /// Artificial delay after a mock Across deposit is indexed before
    /// `/deposit/status` can leave `pending`.
    pub across_status_latency: Duration,
    /// Deterministic probability that an indexed mock Across deposit resolves
    /// as `refunded` instead of `filled`, in basis points.
    pub across_refund_probability_bps: u16,
}

impl Default for MockIntegratorConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            advertised_base_url: None,
            across_spoke_pool_address: None,
            across_evm_rpc_url: None,
            across_chains: BTreeMap::new(),
            mainnet_hyperliquid: false,
            across_api_key: None,
            across_integrator_id: None,
            hyperliquid_bridge_address: None,
            hyperliquid_evm_rpc_url: None,
            hyperliquid_usdc_token_address: None,
            hyperliquid_bridge_deposit_latency: Duration::ZERO,
            hyperliquid_bridge_deposit_failure_probability_bps: 0,
            unit_evm_rpc_urls: BTreeMap::new(),
            unit_withdrawal_release_timeout: DEFAULT_MOCK_UNIT_WITHDRAWAL_RELEASE_TIMEOUT,
            mock_service_evm_chains: BTreeMap::new(),
            mock_service_paymasters: BTreeMap::new(),
            unit_bitcoin_rpc_url: None,
            unit_bitcoin_cookie_path: None,
            cctp_token_messenger_address: None,
            cctp_evm_rpc_url: None,
            cctp_destination_token_address: None,
            cctp_chains: BTreeMap::new(),
            cctp_destination_token_addresses: BTreeMap::new(),
            cctp_attestation_latency: Duration::ZERO,
            cctp_attestation_delay_reason: None,
            address_screening_rules: BTreeMap::new(),
            velora_swap_contract_addresses: BTreeMap::new(),
            velora_transaction_fail_next_n: 0,
            velora_transaction_stale_quote_fail_next_n: 0,
            hyperliquid_withdrawal_latency: Duration::ZERO,
            across_quote_fee_bps: 0,
            across_quote_jitter_bps: 0,
            across_status_latency: Duration::ZERO,
            across_refund_probability_bps: 0,
        }
    }
}

impl MockIntegratorConfig {
    #[must_use]
    pub fn with_bind_addr(mut self, bind_addr: SocketAddr) -> Self {
        self.bind_addr = bind_addr;
        self
    }

    #[must_use]
    pub fn with_advertised_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.advertised_base_url = Some(base_url.into());
        self
    }

    #[must_use]
    pub fn with_across_spoke_pool_address(mut self, address: impl Into<String>) -> Self {
        self.across_spoke_pool_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_across_evm_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.across_evm_rpc_url = Some(url.into());
        self
    }

    #[must_use]
    pub fn with_across_chain(
        mut self,
        origin_chain_id: u64,
        spoke_pool_address: impl Into<String>,
        evm_rpc_url: impl Into<String>,
    ) -> Self {
        self.across_chains.insert(
            origin_chain_id,
            MockAcrossChainConfig {
                spoke_pool_address: spoke_pool_address.into(),
                evm_rpc_url: evm_rpc_url.into(),
            },
        );
        self
    }

    #[must_use]
    pub fn with_mainnet_hyperliquid(mut self, is_mainnet: bool) -> Self {
        self.mainnet_hyperliquid = is_mainnet;
        self
    }

    #[must_use]
    pub fn with_across_auth(
        mut self,
        api_key: impl Into<String>,
        integrator_id: impl Into<String>,
    ) -> Self {
        self.across_api_key = Some(api_key.into());
        self.across_integrator_id = Some(integrator_id.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_bridge_address(mut self, address: impl Into<String>) -> Self {
        self.hyperliquid_bridge_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_evm_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.hyperliquid_evm_rpc_url = Some(url.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_usdc_token_address(mut self, address: impl Into<String>) -> Self {
        self.hyperliquid_usdc_token_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_bridge_deposit_latency(mut self, latency: Duration) -> Self {
        self.hyperliquid_bridge_deposit_latency = latency;
        self
    }

    #[must_use]
    pub fn with_hyperliquid_bridge_deposit_failure_probability_bps(
        mut self,
        probability_bps: u16,
    ) -> Self {
        self.hyperliquid_bridge_deposit_failure_probability_bps = probability_bps.min(10_000);
        self
    }

    #[must_use]
    pub fn with_unit_evm_rpc_url(mut self, chain: UnitChain, url: impl Into<String>) -> Self {
        self.unit_evm_rpc_urls
            .insert(chain.as_wire_str().to_string(), url.into());
        self
    }

    #[must_use]
    pub fn with_unit_withdrawal_release_timeout(mut self, timeout: Duration) -> Self {
        self.unit_withdrawal_release_timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_mock_service_evm_chain(mut self, chain_id: u64, url: impl Into<String>) -> Self {
        self.mock_service_evm_chains.insert(chain_id, url.into());
        self
    }

    #[must_use]
    pub fn with_mock_service_paymasters(
        mut self,
        paymasters: BTreeMap<(MockService, u64), MockServicePaymaster>,
    ) -> Self {
        self.mock_service_paymasters = paymasters;
        self
    }

    #[must_use]
    pub fn with_unit_bitcoin_rpc(
        mut self,
        url: impl Into<String>,
        cookie_path: impl Into<PathBuf>,
    ) -> Self {
        self.unit_bitcoin_rpc_url = Some(url.into());
        self.unit_bitcoin_cookie_path = Some(cookie_path.into());
        self
    }

    #[must_use]
    pub fn with_cctp_token_messenger_address(mut self, address: impl Into<String>) -> Self {
        self.cctp_token_messenger_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_cctp_evm_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.cctp_evm_rpc_url = Some(url.into());
        self
    }

    #[must_use]
    pub fn with_cctp_destination_token_address(mut self, address: impl Into<String>) -> Self {
        self.cctp_destination_token_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_cctp_chain(
        mut self,
        source_chain_id: u64,
        token_messenger_address: impl Into<String>,
        evm_rpc_url: impl Into<String>,
    ) -> Self {
        self.cctp_chains.insert(
            source_chain_id,
            MockCctpChainConfig {
                token_messenger_address: token_messenger_address.into(),
                evm_rpc_url: evm_rpc_url.into(),
            },
        );
        self
    }

    #[must_use]
    pub fn with_cctp_destination_token(
        mut self,
        destination_chain_id: u64,
        token_address: impl Into<String>,
    ) -> Self {
        if let Some(domain) = cctp_domain_for_chain_id(destination_chain_id) {
            self.cctp_destination_token_addresses
                .insert(domain, token_address.into());
        }
        self
    }

    #[must_use]
    pub fn with_cctp_attestation_latency(mut self, latency: Duration) -> Self {
        self.cctp_attestation_latency = latency;
        self
    }

    #[must_use]
    pub fn with_cctp_attestation_delay_reason(mut self, reason: MockCctpDelayReason) -> Self {
        self.cctp_attestation_delay_reason = Some(reason);
        self
    }

    #[must_use]
    pub fn with_address_screening_risk(
        mut self,
        address: impl AsRef<str>,
        risk: MockAddressRiskLevel,
        reason: Option<impl Into<String>>,
    ) -> Self {
        self.address_screening_rules.insert(
            normalize_mock_screening_address(address.as_ref()),
            MockAddressScreeningRule::Risk {
                risk,
                reason: reason.map(Into::into),
            },
        );
        self
    }

    #[must_use]
    pub fn with_address_screening_error(
        mut self,
        address: impl AsRef<str>,
        status: u16,
        body: impl Into<String>,
    ) -> Self {
        self.address_screening_rules.insert(
            normalize_mock_screening_address(address.as_ref()),
            MockAddressScreeningRule::HttpError {
                status,
                body: body.into(),
            },
        );
        self
    }

    #[must_use]
    pub fn with_velora_swap_contract_address(
        mut self,
        chain_id: u64,
        address: impl Into<String>,
    ) -> Self {
        self.velora_swap_contract_addresses
            .insert(chain_id, address.into());
        self
    }

    #[must_use]
    pub fn with_velora_transaction_fail_next_n(mut self, failures: usize) -> Self {
        self.velora_transaction_fail_next_n = failures;
        self
    }

    #[must_use]
    pub fn with_velora_transaction_stale_quote_fail_next_n(mut self, failures: usize) -> Self {
        self.velora_transaction_stale_quote_fail_next_n = failures;
        self
    }

    #[must_use]
    pub fn with_hyperliquid_withdrawal_latency(mut self, latency: Duration) -> Self {
        self.hyperliquid_withdrawal_latency = latency;
        self
    }

    #[must_use]
    pub fn with_across_quote_fee_bps(mut self, fee_bps: u16) -> Self {
        self.across_quote_fee_bps = fee_bps.min(9_999);
        self
    }

    #[must_use]
    pub fn with_across_quote_jitter_bps(mut self, jitter_bps: u16) -> Self {
        self.across_quote_jitter_bps = jitter_bps.min(9_999);
        self
    }

    #[must_use]
    pub fn with_across_status_latency(mut self, latency: Duration) -> Self {
        self.across_status_latency = latency;
        self
    }

    #[must_use]
    pub fn with_across_refund_probability_bps(mut self, probability_bps: u16) -> Self {
        self.across_refund_probability_bps = probability_bps.min(10_000);
        self
    }
}

/// Genuinely shared mock infrastructure that spans every venue: the per-chain
/// service EVM RPC URLs, the deterministic per-service multichain accounts, the
/// per-(service, chain) paymaster handles, and the cross-cutting balance ledger
/// (bridged / Hyperliquid / recipient balances surfaced via `ledger_snapshot`).
pub(crate) struct SharedMockState {
    pub(crate) mock_service_evm_chains: BTreeMap<u64, String>,
    pub(crate) mock_service_accounts: BTreeMap<MockService, MultichainAccount>,
    /// `(service, chain_id) -> paymaster handle`.
    pub(crate) mock_service_paymasters: BTreeMap<(MockService, u64), MockServicePaymaster>,
    pub(crate) ledger: Mutex<MockIntegratorLedger>,
}

/// Thin composition of the per-venue mock states. Each venue owns its own
/// config/state behind an `Arc` (anticipating later axum substate threading),
/// the Hyperliquid account ledger lives in its own `Arc<HyperliquidCore>`, and
/// genuinely cross-cutting infra lives on [`SharedMockState`].
pub(crate) struct MockIntegratorState {
    pub(crate) across: Arc<AcrossMockState>,
    pub(crate) cctp: Arc<CctpMockState>,
    pub(crate) velora: Arc<VeloraMockState>,
    pub(crate) unit: Arc<UnitMockState>,
    pub(crate) hyperliquid: Arc<HyperliquidVenueState>,
    /// The Hyperliquid devnet "chain" state, shared by `Arc` across every
    /// settlement path (spot/exchange handlers, the Bridge2 deposit indexer,
    /// and HyperUnit deposit/withdrawal completion). A single instance is
    /// constructed once in [`MockIntegratorState::new`] and handed to all three
    /// venues via this `Arc` — no venue reaches into another's state.
    // TODO(step: RiftDevnet ownership): the eventual target is for `RiftDevnet`
    // to own `hyperliquid: Arc<HyperliquidCore>` as a devnet chain peer (like
    // `bitcoin` / `ethereum`) and pass it into the mock spawn. That requires
    // threading the Arc through `MockIntegratorServer::spawn_with_config` and
    // every one of its ~58 call sites, so for this step the Arc is owned here
    // and shared; the single-instance guarantee already holds.
    pub(crate) hyperliquid_core: Arc<HyperliquidCore>,
    /// The Coinbase spot-price mock currently serves only static prices, so
    /// nothing reads this yet; it is held for uniformity with the other venue
    /// states and as the home for future Coinbase tunables.
    #[allow(dead_code)]
    pub(crate) coinbase: Arc<CoinbaseMockState>,
    pub(crate) chainalysis: Arc<ChainalysisMockState>,
    pub(crate) shared: Arc<SharedMockState>,
}

pub(crate) type AcrossDepositKey = (u64, String);

#[derive(Default)]
pub(crate) struct MockIntegratorLedger {
    bridged_balances: BTreeMap<String, u128>,
    hyperliquid_balances: BTreeMap<String, u128>,
    recipient_balances: BTreeMap<String, u128>,
}

pub struct MockIntegratorServer {
    base_url: String,
    pub(crate) state: Arc<MockIntegratorState>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    across_indexer_shutdowns: Vec<tokio::sync::oneshot::Sender<()>>,
    across_indexer_handles: Vec<JoinHandle<()>>,
    hyperliquid_indexer_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    hyperliquid_indexer_handle: Option<JoinHandle<()>>,
    unit_deposit_indexer_shutdowns: Vec<tokio::sync::oneshot::Sender<()>>,
    unit_deposit_indexer_handles: Vec<JoinHandle<()>>,
    cctp_indexer_shutdowns: Vec<tokio::sync::oneshot::Sender<()>>,
    cctp_indexer_handles: Vec<JoinHandle<()>>,
}

// Per-venue routers. Each owns its endpoints relative to its mount prefix and
// stays `Router<Arc<MockIntegratorState>>` (state-awaiting) so they can be
// `nest`ed into the root, which provides the shared state exactly once.
fn across_router() -> Router<Arc<MockIntegratorState>> {
    Router::new()
        .route("/swap/approval", get(mock_across_real_swap_approval))
        .route("/deposit/status", get(mock_across_deposit_status))
}

fn cctp_router() -> Router<Arc<MockIntegratorState>> {
    Router::new()
        .route("/v2/messages/:source_domain", get(mock_cctp_messages))
        .route(
            "/v2/burn/USDC/fees/:source_domain/:destination_domain",
            get(mock_cctp_burn_fees),
        )
}

fn velora_router() -> Router<Arc<MockIntegratorState>> {
    Router::new()
        .route("/prices", get(mock_velora_prices))
        .route("/transactions/:network", post(mock_velora_transaction))
}

fn hyperunit_router() -> Router<Arc<MockIntegratorState>> {
    Router::new()
        .route(
            "/gen/:src_chain/:dst_chain/:asset/:dst_addr",
            get(mock_unit_gen),
        )
        .route("/operations/:address", get(mock_unit_operations))
}

fn hyperliquid_router() -> Router<Arc<MockIntegratorState>> {
    Router::new()
        .route("/info", post(mock_hyperliquid_info))
        .route("/exchange", post(mock_hyperliquid_exchange))
}

fn coinbase_router() -> Router<Arc<MockIntegratorState>> {
    Router::new().route(
        "/v2/prices/:currency_pair/spot",
        get(mock_coinbase_spot_price),
    )
}

fn chainalysis_router() -> Router<Arc<MockIntegratorState>> {
    Router::new().route(
        "/api/risk/v2/entities/:address",
        get(mock_chainalysis_address_risk),
    )
}

impl MockIntegratorServer {
    pub async fn spawn() -> eyre::Result<Self> {
        Self::spawn_with_config(MockIntegratorConfig::default()).await
    }

    pub async fn spawn_with_config(config: MockIntegratorConfig) -> eyre::Result<Self> {
        let listener = TcpListener::bind(config.bind_addr).await?;
        let addr = listener.local_addr()?;
        let config = if config.mock_service_paymasters.is_empty()
            && !config.mock_service_evm_chains.is_empty()
        {
            let accounts = mock_service_accounts();
            let paymasters = crate::evm_devnet::setup_mock_service_paymasters(
                &config.mock_service_evm_chains,
                &accounts,
            )
            .await?;
            config.with_mock_service_paymasters(paymasters)
        } else {
            config
        };
        let state = Arc::new(MockIntegratorState::new(&config));

        let (across_indexer_shutdowns, across_indexer_handles) =
            maybe_spawn_across_deposit_indexer(&config, state.clone()).await?;
        let (hyperliquid_indexer_shutdown, hyperliquid_indexer_handle) =
            maybe_spawn_hyperliquid_bridge_indexer(&config, state.clone()).await?;
        let (unit_deposit_indexer_shutdowns, unit_deposit_indexer_handles) =
            maybe_spawn_unit_deposit_indexers(&config, state.clone()).await?;
        let (cctp_indexer_shutdowns, cctp_indexer_handles) =
            maybe_spawn_cctp_burn_indexer(&config, state.clone()).await?;

        // Each venue mounts its own router under a dedicated path prefix, so the
        // namespaces can never collide (today the flat table only avoids
        // collisions by luck — e.g. CCTP's `/v2/messages` vs Coinbase's
        // `/v2/prices`). State is provided once, at the root, after nesting.
        let app = Router::new()
            .nest("/across", across_router())
            .nest("/cctp", cctp_router())
            .nest("/velora", velora_router())
            .nest("/hyperunit", hyperunit_router())
            .nest("/hyperliquid", hyperliquid_router())
            .nest("/coinbase", coinbase_router())
            .nest("/chainalysis", chainalysis_router())
            .with_state(state.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                tracing::warn!(%error, "mock integrator server exited with error");
            }
        });

        Ok(Self {
            base_url: config.advertised_base_url.unwrap_or_else(|| base_url(addr)),
            state,
            shutdown: Some(shutdown_tx),
            handle,
            across_indexer_shutdowns,
            across_indexer_handles,
            hyperliquid_indexer_shutdown,
            hyperliquid_indexer_handle,
            unit_deposit_indexer_shutdowns,
            unit_deposit_indexer_handles,
            cctp_indexer_shutdowns,
            cctp_indexer_handles,
        })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    // Per-venue base URLs: the shared base plus each venue's dedicated path
    // prefix (matching the `nest()` mounts). Callers pass the venue-specific URL
    // to that venue's client instead of guessing with the flat `base_url()`.
    #[must_use]
    pub fn across_url(&self) -> String {
        format!("{}/across", self.base_url)
    }
    #[must_use]
    pub fn cctp_url(&self) -> String {
        format!("{}/cctp", self.base_url)
    }
    #[must_use]
    pub fn velora_url(&self) -> String {
        format!("{}/velora", self.base_url)
    }
    #[must_use]
    pub fn hyperunit_url(&self) -> String {
        format!("{}/hyperunit", self.base_url)
    }
    #[must_use]
    pub fn hyperliquid_url(&self) -> String {
        format!("{}/hyperliquid", self.base_url)
    }
    #[must_use]
    pub fn coinbase_url(&self) -> String {
        format!("{}/coinbase", self.base_url)
    }
    #[must_use]
    pub fn chainalysis_url(&self) -> String {
        format!("{}/chainalysis", self.base_url)
    }

    pub async fn unit_generate_address_requests(&self) -> Vec<MockUnitGenerateAddressRequest> {
        self.state
            .unit
            .generate_address_requests
            .lock()
            .await
            .clone()
    }

    pub async fn hyperliquid_exchange_submissions(&self) -> Vec<Value> {
        self.state
            .hyperliquid
            .exchange_submissions
            .lock()
            .await
            .clone()
    }

    pub async fn unit_operations(&self) -> Vec<MockUnitOperationRecord> {
        snapshot_unit_operations(&*self.state.unit.operations.lock().await)
    }

    pub async fn unit_protocol_private_key(&self, protocol_address: &str) -> Option<String> {
        self.state
            .unit
            .protocol_private_keys
            .lock()
            .await
            .get(protocol_address)
            .map(|key| key.private_key.clone())
    }

    pub async fn ledger_snapshot(&self) -> MockIntegratorLedgerSnapshot {
        let exchange_submissions = self
            .state
            .hyperliquid
            .exchange_submissions
            .lock()
            .await
            .clone();
        let unit_operations = snapshot_unit_operations(&*self.state.unit.operations.lock().await);
        let cctp_burns = self
            .state
            .cctp
            .burns
            .lock()
            .await
            .values()
            .cloned()
            .collect();
        self.state
            .shared
            .ledger
            .lock()
            .await
            .snapshot(exchange_submissions, unit_operations, cctp_burns)
    }

    pub async fn fail_next_across_swap_approval(&self, error: impl Into<String>) {
        *self.state.across.next_swap_approval_error.lock().await = Some(error.into());
    }

    pub async fn fail_next_unit_generate_address(&self, error: impl Into<String>) {
        *self.state.unit.next_generate_address_error.lock().await = Some(error.into());
    }

    pub async fn fail_next_unit_withdrawal_completion(&self, error: impl Into<String>) {
        *self
            .state
            .unit
            .next_withdrawal_completion_error
            .lock()
            .await = Some(error.into());
    }

    pub async fn fail_next_hyperliquid_exchange(&self, error: impl Into<String>) {
        *self.state.hyperliquid.next_exchange_error.lock().await = Some(error.into());
    }

    /// Advance a tracked unit operation to `done`. Tests use this to simulate
    /// HyperUnit finishing a deposit (funds credited to the HL protocol_address)
    /// or a withdrawal (tokens paid out to `dst_addr` after the spotSend).
    pub async fn complete_unit_operation(&self, protocol_address: &str) -> Result<(), String> {
        complete_mock_unit_operation(&self.state, protocol_address).await
    }

    pub async fn complete_unit_operation_with_source_amount(
        &self,
        protocol_address: &str,
        source_amount: impl Into<String>,
    ) -> Result<(), String> {
        complete_mock_unit_operation_with_observation(
            &self.state,
            protocol_address,
            Some(UnitOperationObservation {
                source_amount: source_amount.into(),
                source_tx_hash: None,
            }),
        )
        .await
    }

    /// Move a tracked unit operation into terminal `failure`.
    pub async fn fail_unit_operation(
        &self,
        protocol_address: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        fail_mock_unit_operation(&self.state, protocol_address, error.into()).await
    }

    /// Directly seed a completed Unit operation without going through `/gen`.
    /// Tests that construct synthetic provider operations use this to make the
    /// mock's `/operations/{protocol_address}` return a `done` entry so the
    /// status-hint verifier transitions the operation to `Completed`.
    pub async fn seed_completed_unit_operation(
        &self,
        protocol_address: &str,
        kind: MockUnitOperationKind,
    ) {
        let now = Utc::now().to_rfc3339();
        let entry = MockUnitOperationEntry {
            kind,
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: String::new(),
            visible: true,
            operation: UnitOperation {
                operation_id: Some(format!("0x{}:0", "11".repeat(32))),
                op_created_at: Some(now.clone()),
                protocol_address: Some(protocol_address.to_string()),
                source_address: None,
                destination_address: None,
                source_chain: Some("ethereum".to_string()),
                destination_chain: Some("hyperliquid".to_string()),
                source_amount: Some("1000".to_string()),
                destination_fee_amount: Some("1".to_string()),
                sweep_fee_amount: Some("1".to_string()),
                state: Some("done".to_string()),
                source_tx_hash: Some(format!("0x{}:0", "11".repeat(32))),
                destination_tx_hash: Some(format!(
                    "0x{}:{}",
                    "22".repeat(20),
                    Utc::now().timestamp_millis()
                )),
                source_tx_confirmations: None,
                destination_tx_confirmations: Some(MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS),
                position_in_withdraw_queue: None,
                broadcast_at: Some(now.clone()),
                asset: Some("eth".to_string()),
                state_started_at: Some(now.clone()),
                state_updated_at: Some(now),
                state_next_attempt_at: None,
            },
        };
        self.state
            .unit
            .operations
            .lock()
            .await
            .entry(protocol_address.to_string())
            .or_default()
            .push(entry);
    }

    pub async fn unit_operation_state(&self, protocol_address: &str) -> Option<String> {
        self.state
            .unit
            .operations
            .lock()
            .await
            .get(protocol_address)
            .and_then(|entries| entries.last())
            .and_then(|entry| entry.operation.state.clone())
    }

    pub async fn across_deposits(&self) -> Vec<MockAcrossDepositRecord> {
        self.state
            .across
            .deposits
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn cctp_burns(&self) -> Vec<MockCctpBurnRecord> {
        self.state
            .cctp
            .burns
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }

    /// Returns only the deposits whose `depositor` matches the provided EVM
    /// address. Use this when parallel tests share an anvil spoke pool — each
    /// test's source custody vault has a unique address.
    pub async fn across_deposits_from(&self, depositor: Address) -> Vec<MockAcrossDepositRecord> {
        let padded = FixedBytes::<32>::left_padding_from(depositor.as_slice());
        self.state
            .across
            .deposits
            .lock()
            .await
            .values()
            .filter(|record| record.depositor == padded)
            .cloned()
            .collect()
    }

    /// Credit `amount` of `coin` (e.g. "USDC", "UBTC") to `user`'s mock
    /// Hyperliquid spot balance. Used by tests to fund an account before
    /// placing orders, without going through a deposit flow.
    pub async fn credit_hyperliquid_balance(&self, user: Address, coin: &str, amount: f64) {
        self.state
            .hyperliquid_core
            .lock()
            .await
            .credit_spot(user, coin, amount);
    }

    /// Credit `amount` of `coin` to the mock clearinghouse ledger. This is
    /// the balance surface real Bridge2 deposits land on before a
    /// `usdClassTransfer` moves funds into spot.
    pub async fn credit_hyperliquid_clearinghouse_balance(
        &self,
        user: Address,
        coin: &str,
        amount: f64,
    ) {
        self.state
            .hyperliquid_core
            .lock()
            .await
            .credit_clearinghouse(user, coin, amount);
    }

    /// Read the `total` balance for `user`/`coin`. Returns 0.0 when absent.
    pub async fn hyperliquid_balance_of(&self, user: Address, coin: &str) -> f64 {
        self.state.hyperliquid_core.lock().await.spot_total(user, coin)
    }

    /// Read the clearinghouse balance for `user`/`coin`. Returns 0.0 when
    /// absent.
    pub async fn hyperliquid_clearinghouse_balance_of(&self, user: Address, coin: &str) -> f64 {
        self.state
            .hyperliquid_core
            .lock()
            .await
            .clearinghouse_total(user, coin)
    }

    /// Seed a historical Hyperliquid fill for `/info { type:
    /// "userFillsByTime" }` and `/info { type: "userFills" }` shape tests.
    pub async fn record_hyperliquid_fill(&self, user: Address, fill: UserFill) {
        self.state.hyperliquid_core.lock().await.record_fill(user, fill);
    }

    /// Seed a non-funding ledger update for `/info { type:
    /// "userNonFundingLedgerUpdates" }`.
    pub async fn record_hyperliquid_ledger_update(
        &self,
        user: Address,
        update: UserNonFundingLedgerUpdate,
    ) {
        self.state
            .hyperliquid_core
            .lock()
            .await
            .record_ledger_update(user, update);
    }

    /// Seed a funding payment for `/info { type: "userFunding" }`.
    pub async fn record_hyperliquid_funding(&self, user: Address, funding: UserFunding) {
        self.state
            .hyperliquid_core
            .lock()
            .await
            .record_funding(user, funding);
    }

    /// Install (or overwrite) the exchange rate for a spot pair. `rate` is the
    /// price of one `base` unit in `quote` units (e.g. `set_hyperliquid_rate
    /// ("UBTC", "USDC", 60_000.0)` means 1 UBTC = 60 000 USDC).
    ///
    /// An IoC buy of `sz` base at limit_px ≥ rate fills at exactly `rate`,
    /// debiting `sz * rate` quote and crediting `sz` base. A sell mirrors the
    /// direction.
    pub async fn set_hyperliquid_rate(&self, base: &str, quote: &str, rate: f64) {
        self.state
            .hyperliquid_core
            .lock()
            .await
            .set_rate(base, quote, rate);
    }

    /// Override the synthesized top-of-book depth for both bid and ask sides
    /// of a spot pair. Useful for testing partial-fill semantics.
    pub async fn set_hyperliquid_book_depth(&self, base: &str, quote: &str, depth: f64) {
        self.state
            .hyperliquid_core
            .lock()
            .await
            .set_book_depth(base, quote, depth);
    }

    /// Inspect the configured rate for a pair (returns `None` if none installed).
    pub async fn hyperliquid_rate(&self, base: &str, quote: &str) -> Option<f64> {
        self.state.hyperliquid_core.lock().await.rate_for(base, quote)
    }

    /// Install (or overwrite) the mock Velora USD price in micro-dollars per
    /// whole token. Defaults are ETH=$3,000, BTC=$100,000, USDC=$1, USDT=$1.
    pub async fn set_velora_usd_price_micro(&self, symbol: &str, usd_micro: u128) {
        self.state
            .velora
            .usd_prices
            .lock()
            .await
            .insert(symbol.to_ascii_uppercase(), usd_micro);
    }

    pub async fn set_velora_transaction_fail_next_n(&self, failures: usize) {
        *self
            .state
            .velora
            .transaction_failures_remaining
            .lock()
            .await = failures;
    }

    pub async fn set_velora_transaction_stale_quote_fail_next_n(&self, failures: usize) {
        *self
            .state
            .velora
            .transaction_stale_quote_failures_remaining
            .lock()
            .await = failures;
    }
}

impl MockIntegratorState {
    pub(crate) fn new(config: &MockIntegratorConfig) -> Self {
        Self {
            across: Arc::new(AcrossMockState {
                spoke_pool_address: config.across_spoke_pool_address.clone(),
                evm_rpc_url: config.across_evm_rpc_url.clone(),
                chains: config.across_chains.clone(),
                deposits: Mutex::default(),
                destination_credit_tx_hashes: Mutex::default(),
                api_key: config.across_api_key.clone(),
                integrator_id: config.across_integrator_id.clone(),
                quote_fee_bps: config.across_quote_fee_bps.min(9_999),
                quote_jitter_bps: config.across_quote_jitter_bps.min(9_999),
                status_latency: config.across_status_latency,
                refund_probability_bps: config.across_refund_probability_bps.min(10_000),
                next_swap_approval_error: Mutex::default(),
            }),
            cctp: Arc::new(CctpMockState {
                burns: Mutex::default(),
                attestation_latency: config.cctp_attestation_latency,
                attestation_delay_reason: config.cctp_attestation_delay_reason,
            }),
            velora: Arc::new(VeloraMockState {
                usd_prices: Mutex::new(default_velora_usd_prices()),
                swap_contract_addresses: config.velora_swap_contract_addresses.clone(),
                transaction_failures_remaining: Mutex::new(config.velora_transaction_fail_next_n),
                transaction_stale_quote_failures_remaining: Mutex::new(
                    config.velora_transaction_stale_quote_fail_next_n,
                ),
            }),
            unit: Arc::new(UnitMockState {
                generate_address_requests: Mutex::default(),
                operations: Mutex::default(),
                protocol_private_keys: Mutex::default(),
                evm_rpc_urls: config.unit_evm_rpc_urls.clone(),
                withdrawal_release_timeout: config.unit_withdrawal_release_timeout,
                bitcoin_rpc_url: config.unit_bitcoin_rpc_url.clone(),
                bitcoin_cookie_path: config.unit_bitcoin_cookie_path.clone(),
                next_generate_address_error: Mutex::default(),
                next_withdrawal_completion_error: Mutex::default(),
            }),
            hyperliquid: Arc::new(HyperliquidVenueState {
                bridge_address: config.hyperliquid_bridge_address.clone(),
                evm_rpc_url: config.hyperliquid_evm_rpc_url.clone(),
                usdc_token_address: config.hyperliquid_usdc_token_address.clone(),
                bridge_deposit_latency: config.hyperliquid_bridge_deposit_latency,
                bridge_deposit_failure_probability_bps: config
                    .hyperliquid_bridge_deposit_failure_probability_bps
                    .min(10_000),
                exchange_submissions: Mutex::default(),
                withdrawal_latency: config.hyperliquid_withdrawal_latency,
                next_exchange_error: Mutex::default(),
                mainnet: config.mainnet_hyperliquid,
            }),
            hyperliquid_core: Arc::new(HyperliquidCore::new()),
            coinbase: Arc::new(CoinbaseMockState),
            chainalysis: Arc::new(ChainalysisMockState {
                address_screening_rules: config.address_screening_rules.clone(),
            }),
            shared: Arc::new(SharedMockState {
                mock_service_evm_chains: config.mock_service_evm_chains.clone(),
                mock_service_accounts: mock_service_accounts(),
                mock_service_paymasters: config.mock_service_paymasters.clone(),
                ledger: Mutex::default(),
            }),
        }
    }

    pub(crate) fn mock_service_paymaster(
        &self,
        service: MockService,
        chain_id: u64,
    ) -> Result<&MockServicePaymaster, String> {
        self.shared
            .mock_service_paymasters
            .get(&(service, chain_id))
            .ok_or_else(|| {
                let evm_address = self
                    .shared
                    .mock_service_accounts
                    .get(&service)
                    .map(|account| format!("{:#x}", account.ethereum_address))
                    .unwrap_or_else(|| "<unknown>".to_string());
                format!(
                    "missing mock {} paymaster for EVM chain {chain_id} (service EOA {evm_address})",
                    service.display_name()
                )
            })
    }

    pub(crate) fn mock_service_paymaster_rpc_url(&self, chain_id: u64) -> Option<&str> {
        self.shared
            .mock_service_evm_chains
            .get(&chain_id)
            .map(String::as_str)
    }

    pub(crate) fn across_chain_config(
        &self,
        origin_chain_id: u64,
    ) -> Option<ResolvedMockAcrossChainConfig> {
        self.across
            .chains
            .get(&origin_chain_id)
            .map(|config| ResolvedMockAcrossChainConfig {
                spoke_pool_address: config.spoke_pool_address.clone(),
                evm_rpc_url: Some(config.evm_rpc_url.clone()),
            })
            .or_else(|| {
                Some(ResolvedMockAcrossChainConfig {
                    spoke_pool_address: self.across.spoke_pool_address.clone()?,
                    evm_rpc_url: self.across.evm_rpc_url.clone(),
                })
            })
    }

    pub(crate) fn across_chain_rpc_url(&self, chain_id: u64) -> Option<String> {
        self.across
            .chains
            .get(&chain_id)
            .map(|config| config.evm_rpc_url.clone())
            .or_else(|| {
                if self.across.chains.is_empty() {
                    self.across.evm_rpc_url.clone()
                } else {
                    None
                }
            })
    }
}

pub fn mock_service_accounts() -> BTreeMap<MockService, MultichainAccount> {
    MockService::ALL
        .into_iter()
        .map(|service| {
            let account = MultichainAccount::new(service.salt()).unwrap_or_else(|error| {
                panic!(
                    "failed to derive deterministic {} mock service account from salt {}: {error}",
                    service.display_name(),
                    service.salt()
                )
            });
            (service, account)
        })
        .collect()
}

impl Drop for MockIntegratorServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        for shutdown in self.across_indexer_shutdowns.drain(..) {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.hyperliquid_indexer_shutdown.take() {
            let _ = shutdown.send(());
        }
        for shutdown in self.unit_deposit_indexer_shutdowns.drain(..) {
            let _ = shutdown.send(());
        }
        for shutdown in self.cctp_indexer_shutdowns.drain(..) {
            let _ = shutdown.send(());
        }
        self.handle.abort();
        for handle in self.across_indexer_handles.drain(..) {
            handle.abort();
        }
        if let Some(handle) = self.hyperliquid_indexer_handle.take() {
            handle.abort();
        }
        for handle in self.unit_deposit_indexer_handles.drain(..) {
            handle.abort();
        }
        for handle in self.cctp_indexer_handles.drain(..) {
            handle.abort();
        }
    }
}

impl MockIntegratorLedger {
    fn snapshot(
        &self,
        hyperliquid_exchange_submissions: Vec<Value>,
        unit_operations: Vec<MockUnitOperationRecord>,
        cctp_burns: Vec<MockCctpBurnRecord>,
    ) -> MockIntegratorLedgerSnapshot {
        MockIntegratorLedgerSnapshot {
            bridged_balances: stringify_balances(&self.bridged_balances),
            hyperliquid_balances: stringify_balances(&self.hyperliquid_balances),
            recipient_balances: stringify_balances(&self.recipient_balances),
            unit_operations,
            cctp_burns,
            hyperliquid_exchange_submissions,
        }
    }
}

pub(crate) fn deterministic_bps(seed: &str, max_bps: u16) -> u16 {
    if max_bps == 0 {
        return 0;
    }
    let digest = Sha256::digest(seed.as_bytes());
    let raw = u16::from_be_bytes([digest[0], digest[1]]);
    raw % (max_bps.saturating_add(1))
}

pub(crate) fn address_to_bytes32(address: Address) -> FixedBytes<32> {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    FixedBytes::<32>::from(bytes)
}

pub(crate) async fn mock_credit_native_on_anvil(
    state: &MockIntegratorState,
    chain_id: u64,
    service: MockService,
    recipient: Address,
    amount: U256,
) -> Result<(), String> {
    let handle = state.mock_service_paymaster(service, chain_id)?;
    let execution = Execution {
        target: recipient,
        value: amount,
        callData: Bytes::new(),
    };
    handle
        .submit(execution)
        .await
        .map(|_| ())
        .map_err(|err| format!("mock native credit submit failed: {err}"))
}

pub(crate) async fn mock_mint_erc20_on_anvil(
    state: &MockIntegratorState,
    chain_id: u64,
    service: MockService,
    token: Address,
    recipient: Address,
    amount: U256,
) -> Result<String, String> {
    let handle = state.mock_service_paymaster(service, chain_id)?;
    let provider = ProviderBuilder::new()
        .connect(
            state
                .mock_service_paymaster_rpc_url(chain_id)
                .ok_or_else(|| format!("mock service EVM chain {chain_id} has no RPC URL"))?,
        )
        .await
        .map_err(|err| err.to_string())?;
    let code = provider
        .get_code_at(token)
        .await
        .map_err(|err| err.to_string())?;
    if code.is_empty() {
        return Err(format!("destination token {token:#x} is not deployed"));
    }
    let calldata = Bytes::from(IMockMintableERC20::mintCall { recipient, amount }.abi_encode());
    let execution = Execution {
        target: token,
        value: U256::ZERO,
        callData: calldata,
    };
    let tx_hash = handle
        .submit(execution)
        .await
        .map_err(|err| format!("mock mint submit failed: {err}"))?;
    Ok(format!("{tx_hash:#x}"))
}
pub(crate) fn mock_evm_indexer_initial_last_scanned() -> u64 {
    0
}
pub(crate) fn bytes32_to_evm_address(value: FixedBytes<32>) -> Address {
    Address::from_slice(&value.as_slice()[12..])
}

#[cfg(test)]
fn sanitize_url_for_error(value: &str) -> String {
    let Ok(parsed) = url::Url::parse(value.trim()) else {
        return "<invalid url>".to_string();
    };

    let host = parsed.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", parsed.scheme(), host);
    if let Some(port) = parsed.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if parsed.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if parsed.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    if parsed.fragment().is_some() {
        redacted.push_str("#<redacted-fragment>");
    }
    redacted
}

fn base_url(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

pub(crate) fn parse_amount(field: &str, value: &str) -> Result<u128, String> {
    let amount = value
        .parse::<u128>()
        .map_err(|error| format!("{field} must be an integer base-unit amount: {error}"))?;
    if amount == 0 {
        return Err(format!("{field} must be nonzero"));
    }
    Ok(amount)
}

fn stringify_balances(balances: &BTreeMap<String, u128>) -> BTreeMap<String, String> {
    balances
        .iter()
        .map(|(key, value)| (key.clone(), value.to_string()))
        .collect()
}

pub(crate) fn error_response(
    status: StatusCode,
    error: impl Into<String>,
) -> axum::response::Response {
    (
        status,
        Json(MockIntegratorErrorResponse {
            error: error.into(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn mock_evm_log_indexers_backfill_existing_chain_history() {
        assert_eq!(mock_evm_indexer_initial_last_scanned(), 0);
    }

    #[test]
    fn mock_integrator_url_errors_redact_secret_material() {
        let redacted = sanitize_url_for_error(
            "https://rpc-user:rpc-pass@rpc.example/v2/path-secret?api_key=query-secret#fragment-secret",
        );

        assert_eq!(
            redacted,
            "https://rpc.example/<redacted-path>?<redacted-query>#<redacted-fragment>"
        );
        assert!(!redacted.contains("rpc-user"));
        assert!(!redacted.contains("rpc-pass"));
        assert!(!redacted.contains("path-secret"));
        assert!(!redacted.contains("query-secret"));
        assert!(!redacted.contains("fragment-secret"));

        let invalid = sanitize_url_for_error("not a url with secret-token");
        assert_eq!(invalid, "<invalid url>");
        assert!(!invalid.contains("secret-token"));
    }

    #[test]
    fn mock_service_accounts_are_distinct() {
        let accounts = mock_service_accounts();

        let evm_addresses = accounts
            .values()
            .map(|account| account.ethereum_address)
            .collect::<BTreeSet<_>>();
        let bitcoin_addresses = accounts
            .values()
            .map(|account| account.bitcoin_wallet.address.to_string())
            .collect::<BTreeSet<_>>();

        assert_eq!(accounts.len(), MockService::ALL.len());
        assert_eq!(evm_addresses.len(), MockService::ALL.len());
        assert_eq!(bitcoin_addresses.len(), MockService::ALL.len());
    }

    #[test]
    fn mock_service_paymaster_lookup_keyed_correctly() {
        let chain_id = 8453;
        let paymasters = MockService::ALL
            .into_iter()
            .map(|service| {
                (
                    (service, chain_id),
                    MockServicePaymaster::stub(service, chain_id),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let state = MockIntegratorState::new(
            &MockIntegratorConfig::default()
                .with_mock_service_evm_chain(chain_id, "http://localhost:8545")
                .with_mock_service_paymasters(paymasters),
        );

        let hyperunit = state
            .mock_service_paymaster(MockService::Hyperunit, chain_id)
            .expect("hyperunit paymaster");
        let across = state
            .mock_service_paymaster(MockService::Across, chain_id)
            .expect("across paymaster");

        assert_eq!(hyperunit.service(), MockService::Hyperunit);
        assert_eq!(across.service(), MockService::Across);
        assert_eq!(hyperunit.chain_id(), chain_id);
        assert_eq!(across.chain_id(), chain_id);
        assert_ne!(hyperunit.service(), across.service());
        assert!(state
            .mock_service_paymaster(MockService::Hyperunit, 1)
            .is_err());
    }
}
