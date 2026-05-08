use super::bitcoin_funding::{observed_bitcoin_outpoint, ObservedBitcoinOutpoint};
use crate::{
    config::Settings,
    db::Database,
    error::RouterCoreError,
    models::{
        CustodyVault, CustodyVaultControlType, CustodyVaultRole, CustodyVaultStatus,
        CustodyVaultVisibility,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId},
};
use alloy::{
    primitives::{keccak256, Address, Bytes, U256},
    signers::local::PrivateKeySigner,
};
use bitcoin::secp256k1::Secp256k1;
use bitcoin::{CompressedPublicKey, PrivateKey};
use chains::{evm::EvmBroadcastPolicy, ChainOperations, ChainRegistry};
use hyperliquid_client::{
    actions::Actions as HyperliquidActions, client::Network as HyperliquidNetwork,
    HyperliquidExchangeClient, HyperliquidInfoClient,
};
use router_primitives::ChainType;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use snafu::Snafu;
use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum CustodyActionError {
    #[snafu(display("Database error: {}", source))]
    Database { source: RouterCoreError },

    #[snafu(display("Chain not supported: {}", chain))]
    ChainNotSupported { chain: ChainId },

    #[snafu(display("custody vault {} has no asset for a transfer action", vault_id))]
    MissingTransferAsset { vault_id: Uuid },

    #[snafu(display("custody vault {} is not controlled by a router-derived key", vault_id))]
    UnsupportedControlType {
        vault_id: Uuid,
        control_type: CustodyVaultControlType,
    },

    #[snafu(display("custody vault {} is missing a derivation salt", vault_id))]
    MissingDerivationSalt { vault_id: Uuid },

    #[snafu(display(
        "custody vault {} has invalid bitcoin funding observation: {}",
        vault_id,
        reason
    ))]
    InvalidBitcoinFundingObservation { vault_id: Uuid, reason: String },

    #[snafu(display(
        "derived address {} does not match custody vault {} address {}",
        derived_address,
        vault_id,
        vault_address
    ))]
    CustodyAddressMismatch {
        vault_id: Uuid,
        derived_address: String,
        vault_address: String,
    },

    #[snafu(display("invalid {} amount: {}", field, reason))]
    InvalidAmount { field: &'static str, reason: String },

    #[snafu(display("invalid calldata: {}", reason))]
    InvalidCalldata { reason: String },

    #[snafu(display("serialization failed: {}", reason))]
    Serialization { reason: String },

    #[snafu(display("failed to generate custody vault derivation salt: {}", source))]
    Random { source: getrandom::Error },

    #[snafu(display("unsupported custody action {} on chain {}", action, chain))]
    UnsupportedAction {
        action: &'static str,
        chain: ChainId,
    },

    #[snafu(display("Failed to derive custody wallet: {}", source))]
    WalletDerivation { source: chains::Error },

    #[snafu(display("Chain action failed: {}", source))]
    Chain { source: chains::Error },

    #[snafu(display("invalid hyperliquid wallet: {}", reason))]
    InvalidHyperliquidWallet { reason: String },

    #[snafu(display("invalid hyperliquid vault address: {}", reason))]
    InvalidHyperliquidVaultAddress { reason: String },

    #[snafu(display("hyperliquid client error: {}", source))]
    Hyperliquid { source: hyperliquid_client::Error },

    #[snafu(display("hyperliquid action failed: {}", reason))]
    HyperliquidAction { reason: String },

    #[snafu(display("invalid hyperliquid timestamp for {}: {}", context, reason))]
    InvalidHyperliquidTimestamp {
        context: &'static str,
        reason: String,
    },
}

pub type CustodyActionResult<T> = Result<T, CustodyActionError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum CustodyAction {
    Transfer {
        to_address: String,
        amount: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bitcoin_fee_budget_sats: Option<String>,
    },
    Call(ChainCall),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "chain_type", content = "call", rename_all = "snake_case")]
pub enum ChainCall {
    Evm(EvmCall),
    Hyperliquid(HyperliquidCall),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvmCall {
    pub to_address: String,
    pub value: String,
    pub calldata: String,
    #[serde(default, skip_serializing_if = "is_default_evm_broadcast_policy")]
    pub broadcast_policy: EvmBroadcastPolicy,
}

fn is_default_evm_broadcast_policy(policy: &EvmBroadcastPolicy) -> bool {
    *policy == EvmBroadcastPolicy::Standard
}

/// Hyperliquid exchange action submitted by the custody executor. The vault's
/// derived EVM key signs the action (EIP-712) at submit time, so the nonce
/// binds to the signature — the provider can't pre-sign.
///
/// `target_base_url` points at mainnet/testnet or the devnet mock; `network`
/// selects the L1 signing source byte ("a"/"b") and the `hyperliquidChain`
/// string stamped into user-type actions (Withdraw3, …).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HyperliquidCall {
    pub target_base_url: String,
    pub network: HyperliquidCallNetwork,
    /// Optional HL sub-account/vault address that the action is submitted
    /// *on behalf of*. The signing key still belongs to the custody vault —
    /// this mirrors HL's `vaultAddress` envelope field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault_address: Option<String>,
    pub payload: HyperliquidCallPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HyperliquidCallNetwork {
    Mainnet,
    Testnet,
}

impl From<HyperliquidCallNetwork> for HyperliquidNetwork {
    fn from(value: HyperliquidCallNetwork) -> Self {
        match value {
            HyperliquidCallNetwork::Mainnet => HyperliquidNetwork::Mainnet,
            HyperliquidCallNetwork::Testnet => HyperliquidNetwork::Testnet,
        }
    }
}

/// Wire-shape payload variants. `L1Action` carries a fully-formed L1 action
/// (Order, Cancel, …); user-type actions like Withdraw3 / sendAsset get their
/// own variants because their signing domains and envelopes differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HyperliquidCallPayload {
    L1Action {
        action: HyperliquidActions,
    },
    UsdClassTransfer {
        amount: String,
        to_perp: bool,
    },
    Withdraw3 {
        destination: String,
        amount: String,
    },
    SpotSend {
        destination: String,
        token: String,
        amount: String,
    },
    SendAsset {
        destination: String,
        source_dex: String,
        destination_dex: String,
        token: String,
        amount: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodyActionRequest {
    pub custody_vault_id: Uuid,
    pub action: CustodyAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustodyActionReceipt {
    pub custody_vault_id: Uuid,
    /// On-chain tx hash for EVM / Bitcoin / etc. For Hyperliquid this is the
    /// L1 action hash when the client can derive it, otherwise a synthetic
    /// fallback derived from the HL response.
    pub tx_hash: String,
    /// EVM logs emitted by the transaction, when the action produced them.
    /// Non-EVM-call actions (transfers, non-EVM chains) leave this empty.
    pub logs: Vec<alloy::rpc::types::Log>,
    /// Raw provider response body, when the underlying chain returns JSON
    /// rather than emitting logs (currently: Hyperliquid `/exchange`). Empty
    /// for EVM calls — they surface observable state via `logs`.
    pub response: Option<Value>,
}

#[derive(Clone)]
pub struct CustodyActionExecutor {
    db: Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    hyperliquid_execution: Option<HyperliquidExecutionConfig>,
    hyperliquid_runtime: Option<HyperliquidRuntimeConfig>,
    paymasters: PaymasterRegistry,
}

#[derive(Clone, PartialEq, Eq)]
pub struct HyperliquidExecutionConfig {
    signer_private_key: String,
    signer_address: Address,
    account_address: Option<Address>,
    vault_address: Option<Address>,
}

impl fmt::Debug for HyperliquidExecutionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperliquidExecutionConfig")
            .field("signer_private_key", &"redacted")
            .field("signer_address", &self.signer_address)
            .field("account_address", &self.account_address)
            .field("vault_address", &self.vault_address)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidRuntimeConfig {
    base_url: String,
    network: HyperliquidCallNetwork,
}

impl HyperliquidRuntimeConfig {
    #[must_use]
    pub fn new(base_url: impl Into<String>, network: HyperliquidCallNetwork) -> Self {
        Self {
            base_url: base_url.into(),
            network,
        }
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    #[must_use]
    pub fn network(&self) -> HyperliquidCallNetwork {
        self.network
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PaymasterRegistry {
    addresses: HashMap<ChainType, String>,
}

impl PaymasterRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, chain: ChainType, address: impl Into<String>) {
        self.addresses.insert(chain, address.into());
    }

    #[must_use]
    pub fn address_for(&self, chain: ChainType) -> Option<&str> {
        self.addresses.get(&chain).map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleasedSweepResult {
    Swept { metadata_patch: Value },
    Skipped { metadata_patch: Value },
}

impl ReleasedSweepResult {
    #[must_use]
    pub fn metadata_patch(&self) -> &Value {
        match self {
            Self::Swept { metadata_patch } | Self::Skipped { metadata_patch } => metadata_patch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HyperliquidSpotBalance {
    pub coin: String,
    pub total: String,
    pub hold: String,
}

impl HyperliquidExecutionConfig {
    pub fn new(
        signer_private_key: impl Into<String>,
        account_address: Option<Address>,
        vault_address: Option<Address>,
    ) -> CustodyActionResult<Self> {
        let signer_private_key = signer_private_key.into();
        let signer = signer_private_key
            .trim_start_matches("0x")
            .parse::<PrivateKeySigner>()
            .map_err(|err| CustodyActionError::InvalidHyperliquidWallet {
                reason: err.to_string(),
            })?;
        Ok(Self {
            signer_private_key,
            signer_address: signer.address(),
            account_address,
            vault_address,
        })
    }

    #[must_use]
    pub fn signer_address(&self) -> Address {
        self.signer_address
    }

    #[must_use]
    pub fn account_address(&self) -> Option<Address> {
        self.account_address
    }

    #[must_use]
    pub fn target_address(&self) -> Address {
        self.vault_address
            .or(self.account_address)
            .unwrap_or(self.signer_address)
    }

    #[must_use]
    pub fn submission_vault_address(&self) -> Option<Address> {
        self.vault_address
    }

    #[must_use]
    pub fn signer_wallet(&self) -> router_primitives::Wallet {
        router_primitives::Wallet::new(
            format!("{:#x}", self.signer_address),
            self.signer_private_key.clone(),
        )
    }
}

impl CustodyActionExecutor {
    #[must_use]
    pub fn new(db: Database, settings: Arc<Settings>, chain_registry: Arc<ChainRegistry>) -> Self {
        Self::new_with_hyperliquid_execution(db, settings, chain_registry, None)
    }

    #[must_use]
    pub fn new_with_hyperliquid_execution(
        db: Database,
        settings: Arc<Settings>,
        chain_registry: Arc<ChainRegistry>,
        hyperliquid_execution: Option<HyperliquidExecutionConfig>,
    ) -> Self {
        Self {
            db,
            settings,
            chain_registry,
            hyperliquid_execution,
            hyperliquid_runtime: None,
            paymasters: PaymasterRegistry::default(),
        }
    }

    #[must_use]
    pub fn with_hyperliquid_runtime(
        mut self,
        hyperliquid_runtime: Option<HyperliquidRuntimeConfig>,
    ) -> Self {
        self.hyperliquid_runtime = hyperliquid_runtime;
        self
    }

    #[must_use]
    pub fn with_paymasters(mut self, paymasters: PaymasterRegistry) -> Self {
        self.paymasters = paymasters;
        self
    }

    #[must_use]
    pub fn paymaster_address(&self, chain: ChainType) -> Option<&str> {
        self.paymasters.address_for(chain)
    }

    pub async fn execute(
        &self,
        request: CustodyActionRequest,
    ) -> CustodyActionResult<CustodyActionReceipt> {
        let vault = self
            .db
            .orders()
            .get_custody_vault(request.custody_vault_id)
            .await
            .map_err(|source| CustodyActionError::Database { source })?;
        let wallet = self.derive_wallet(&vault)?;
        let backend_chain = backend_chain_for_id(&vault.chain).ok_or_else(|| {
            CustodyActionError::ChainNotSupported {
                chain: vault.chain.clone(),
            }
        })?;

        let (tx_hash, logs, response) = match request.action {
            CustodyAction::Transfer {
                to_address,
                amount,
                bitcoin_fee_budget_sats,
            } => {
                let amount = parse_positive_u256("amount", &amount)?;
                let bitcoin_fee_budget = bitcoin_fee_budget_sats
                    .as_deref()
                    .map(|value| parse_positive_u256("bitcoin_fee_budget_sats", value))
                    .transpose()?;
                let asset = vault.asset.as_ref().ok_or_else(|| {
                    CustodyActionError::MissingTransferAsset { vault_id: vault.id }
                })?;
                let tx_hash = match asset {
                    AssetId::Native => match backend_chain {
                        ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base => {
                            let Some(evm_chain) = self.chain_registry.get_evm(&backend_chain)
                            else {
                                return Err(CustodyActionError::UnsupportedAction {
                                    action: "native_transfer",
                                    chain: vault.chain,
                                });
                            };
                            evm_chain
                                .ensure_native_gas_for_transaction(
                                    &vault.address,
                                    &to_address,
                                    amount,
                                    Bytes::new(),
                                    "native_transfer",
                                )
                                .await
                                .map_err(|source| CustodyActionError::Chain { source })?;
                            evm_chain
                                .transfer_native_amount(wallet.private_key(), &to_address, amount)
                                .await
                                .map_err(|source| CustodyActionError::Chain { source })?
                        }
                        ChainType::Bitcoin => {
                            let Some(bitcoin_chain) =
                                self.chain_registry.get_bitcoin(&backend_chain)
                            else {
                                return Err(CustodyActionError::UnsupportedAction {
                                    action: "bitcoin_transfer",
                                    chain: vault.chain,
                                });
                            };
                            if let Some(outpoint) =
                                self.observed_bitcoin_source_outpoint(&vault).await?
                            {
                                bitcoin_chain
                                    .transfer_native_amount_from_outpoint(
                                        wallet.private_key(),
                                        &to_address,
                                        amount,
                                        bitcoin_fee_budget,
                                        &outpoint.tx_hash,
                                        outpoint.vout,
                                        outpoint.amount_sats,
                                    )
                                    .await
                                    .map_err(|source| CustodyActionError::Chain { source })?
                            } else {
                                bitcoin_chain
                                    .transfer_native_amount(
                                        wallet.private_key(),
                                        &to_address,
                                        amount,
                                        bitcoin_fee_budget,
                                    )
                                    .await
                                    .map_err(|source| CustodyActionError::Chain { source })?
                            }
                        }
                        ChainType::Hyperliquid => {
                            return Err(CustodyActionError::UnsupportedAction {
                                action: "transfer",
                                chain: vault.chain,
                            });
                        }
                    },
                    AssetId::Reference(token_address) => {
                        let Some(evm_chain) = self.chain_registry.get_evm(&backend_chain) else {
                            return Err(CustodyActionError::UnsupportedAction {
                                action: "erc20_transfer",
                                chain: vault.chain,
                            });
                        };
                        evm_chain
                            .ensure_native_gas_for_erc20_transfer(
                                token_address,
                                &vault.address,
                                &to_address,
                                amount,
                            )
                            .await
                            .map_err(|source| CustodyActionError::Chain { source })?;
                        evm_chain
                            .transfer_erc20_amount(
                                token_address,
                                wallet.private_key(),
                                &to_address,
                                amount,
                            )
                            .await
                            .map_err(|source| CustodyActionError::Chain { source })?
                    }
                };
                (tx_hash, Vec::new(), None)
            }
            CustodyAction::Call(ChainCall::Evm(call)) => {
                let Some(evm_chain) = self.chain_registry.get_evm(&backend_chain) else {
                    return Err(CustodyActionError::UnsupportedAction {
                        action: "evm_call",
                        chain: vault.chain,
                    });
                };
                let value = parse_u256("value", &call.value)?;
                let calldata = decode_calldata(&call.calldata)?;
                evm_chain
                    .ensure_native_gas_for_transaction(
                        &vault.address,
                        &call.to_address,
                        value,
                        calldata.clone(),
                        "evm_call",
                    )
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                let outcome = evm_chain
                    .send_call_with_broadcast_policy(
                        wallet.private_key(),
                        &call.to_address,
                        value,
                        calldata,
                        call.broadcast_policy,
                    )
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                (outcome.tx_hash, outcome.logs, None)
            }
            CustodyAction::Call(ChainCall::Hyperliquid(call)) => {
                if !matches!(
                    backend_chain,
                    ChainType::Ethereum
                        | ChainType::Arbitrum
                        | ChainType::Base
                        | ChainType::Hyperliquid
                ) {
                    return Err(CustodyActionError::UnsupportedAction {
                        action: "hyperliquid_call",
                        chain: vault.chain,
                    });
                }
                let mut effective_call = call.clone();
                if vault.control_type == CustodyVaultControlType::HyperliquidMasterSigner
                    && effective_call.vault_address.is_none()
                {
                    effective_call.vault_address = self
                        .hyperliquid_execution
                        .as_ref()
                        .and_then(HyperliquidExecutionConfig::submission_vault_address)
                        .map(|address| format!("{address:#x}"));
                }
                let (tx_hash, response) =
                    execute_hyperliquid_call(&effective_call, wallet.private_key()).await?;
                (tx_hash, Vec::new(), Some(response))
            }
        };

        Ok(CustodyActionReceipt {
            custody_vault_id: vault.id,
            tx_hash,
            logs,
            response,
        })
    }

    pub async fn create_router_derived_vault(
        &self,
        order_id: Uuid,
        role: CustodyVaultRole,
        visibility: CustodyVaultVisibility,
        chain: ChainId,
        asset: Option<AssetId>,
        metadata: Value,
    ) -> CustodyActionResult<CustodyVault> {
        let backend_chain =
            backend_chain_for_id(&chain).ok_or_else(|| CustodyActionError::ChainNotSupported {
                chain: chain.clone(),
            })?;
        let chain_impl = self.chain_registry.get(&backend_chain).ok_or_else(|| {
            CustodyActionError::ChainNotSupported {
                chain: chain.clone(),
            }
        })?;
        let mut derivation_salt = [0u8; 32];
        getrandom::getrandom(&mut derivation_salt)
            .map_err(|source| CustodyActionError::Random { source })?;
        let wallet = chain_impl
            .derive_wallet(&self.settings.master_key_bytes(), &derivation_salt)
            .map_err(|source| CustodyActionError::WalletDerivation { source })?;
        let now = chrono::Utc::now();
        let vault = CustodyVault {
            id: Uuid::now_v7(),
            order_id: Some(order_id),
            role,
            visibility,
            chain,
            asset,
            address: wallet.address.clone(),
            control_type: CustodyVaultControlType::RouterDerivedKey,
            derivation_salt: Some(derivation_salt),
            signer_ref: None,
            status: CustodyVaultStatus::Active,
            metadata: json_object_or_wrapped(metadata),
            created_at: now,
            updated_at: now,
        };
        self.db
            .orders()
            .create_custody_vault(&vault)
            .await
            .map_err(|source| CustodyActionError::Database { source })?;

        Ok(vault)
    }

    pub async fn sweep_released_internal_custody(
        &self,
        vault: &CustodyVault,
    ) -> CustodyActionResult<ReleasedSweepResult> {
        let attempted_at = chrono::Utc::now().to_rfc3339();
        let backend_chain = backend_chain_for_id(&vault.chain).ok_or_else(|| {
            CustodyActionError::ChainNotSupported {
                chain: vault.chain.clone(),
            }
        })?;
        let Some(paymaster_address) = self.paymaster_address(backend_chain) else {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "missing_paymaster",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                }),
            });
        };

        match backend_chain {
            ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base => {
                self.sweep_released_evm_vault(vault, backend_chain, paymaster_address, attempted_at)
                    .await
            }
            ChainType::Bitcoin => {
                self.sweep_released_bitcoin_vault(vault, paymaster_address, attempted_at)
                    .await
            }
            ChainType::Hyperliquid => {
                self.sweep_released_hyperliquid_vault(vault, paymaster_address, attempted_at)
                    .await
            }
        }
    }

    pub async fn inspect_hyperliquid_spot_balances(
        &self,
        vault: &CustodyVault,
    ) -> CustodyActionResult<Vec<HyperliquidSpotBalance>> {
        if vault.control_type == CustodyVaultControlType::HyperliquidMasterSigner {
            return Ok(Vec::new());
        }
        let Some(runtime) = self.hyperliquid_runtime.as_ref() else {
            return Ok(Vec::new());
        };

        let user = Address::from_str(&vault.address).map_err(|err| {
            CustodyActionError::InvalidHyperliquidWallet {
                reason: err.to_string(),
            }
        })?;
        let mut info = HyperliquidInfoClient::new(runtime.base_url())
            .map_err(|source| CustodyActionError::Hyperliquid { source })?;
        info.refresh_spot_meta()
            .await
            .map_err(|source| CustodyActionError::Hyperliquid { source })?;
        let state = info
            .spot_clearinghouse_state(user)
            .await
            .map_err(|source| CustodyActionError::Hyperliquid { source })?;
        Ok(state
            .balances
            .into_iter()
            .map(|balance| HyperliquidSpotBalance {
                coin: balance.coin,
                total: balance.total,
                hold: balance.hold,
            })
            .collect())
    }

    fn derive_wallet(
        &self,
        vault: &CustodyVault,
    ) -> CustodyActionResult<router_primitives::Wallet> {
        match vault.control_type {
            CustodyVaultControlType::RouterDerivedKey => {
                let salt = vault
                    .derivation_salt
                    .ok_or(CustodyActionError::MissingDerivationSalt { vault_id: vault.id })?;
                let backend_chain = backend_chain_for_id(&vault.chain).ok_or_else(|| {
                    CustodyActionError::ChainNotSupported {
                        chain: vault.chain.clone(),
                    }
                })?;
                let chain = self.chain_registry.get(&backend_chain).ok_or_else(|| {
                    CustodyActionError::ChainNotSupported {
                        chain: vault.chain.clone(),
                    }
                })?;
                let wallet = chain
                    .derive_wallet(&self.settings.master_key_bytes(), &salt)
                    .map_err(|source| CustodyActionError::WalletDerivation { source })?;
                validate_derived_address(vault, &wallet.address)?;
                Ok(wallet)
            }
            CustodyVaultControlType::HyperliquidMasterSigner => self
                .hyperliquid_execution
                .as_ref()
                .ok_or_else(|| CustodyActionError::UnsupportedControlType {
                    vault_id: vault.id,
                    control_type: vault.control_type,
                })
                .map(HyperliquidExecutionConfig::signer_wallet),
            _ => Err(CustodyActionError::UnsupportedControlType {
                vault_id: vault.id,
                control_type: vault.control_type,
            }),
        }
    }

    async fn observed_bitcoin_source_outpoint(
        &self,
        vault: &CustodyVault,
    ) -> CustodyActionResult<Option<ObservedBitcoinOutpoint>> {
        if vault.role != CustodyVaultRole::SourceDeposit || vault.chain.as_str() != "bitcoin" {
            return Ok(None);
        }
        let deposit_vault = self
            .db
            .vaults()
            .get(vault.id)
            .await
            .map_err(|source| CustodyActionError::Database { source })?;
        observed_bitcoin_outpoint(deposit_vault.funding_observation.as_ref()).map_err(|reason| {
            CustodyActionError::InvalidBitcoinFundingObservation {
                vault_id: vault.id,
                reason,
            }
        })
    }

    async fn sweep_released_evm_vault(
        &self,
        vault: &CustodyVault,
        backend_chain: ChainType,
        paymaster_address: &str,
        attempted_at: String,
    ) -> CustodyActionResult<ReleasedSweepResult> {
        let wallet = self.derive_wallet(vault)?;
        let Some(chain) = self.chain_registry.get_evm(&backend_chain) else {
            return Err(CustodyActionError::ChainNotSupported {
                chain: vault.chain.clone(),
            });
        };
        let Some(asset) = vault.asset.as_ref() else {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "missing_asset",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                }),
            });
        };

        match asset {
            AssetId::Native => {
                let balance = chain
                    .native_balance(&vault.address)
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                if balance.is_zero() {
                    return Ok(ReleasedSweepResult::Skipped {
                        metadata_patch: json!({
                            "release_sweep_status": "empty",
                            "release_sweep_terminal": true,
                            "release_sweep_attempted_at": attempted_at,
                            "release_sweep_target_address": paymaster_address,
                        }),
                    });
                }

                let reserved_fee = chain
                    .estimate_native_transfer_fee(&vault.address, paymaster_address, balance)
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                if balance <= reserved_fee {
                    return Ok(ReleasedSweepResult::Skipped {
                        metadata_patch: json!({
                            "release_sweep_status": "uneconomic",
                            "release_sweep_terminal": true,
                            "release_sweep_attempted_at": attempted_at,
                            "release_sweep_target_address": paymaster_address,
                            "release_sweep_balance": balance.to_string(),
                            "release_sweep_reserved_fee": reserved_fee.to_string(),
                        }),
                    });
                }

                let sweep_amount = checked_release_sweep_amount(balance, reserved_fee)?;
                let tx_hash = chain
                    .transfer_native_amount(wallet.private_key(), paymaster_address, sweep_amount)
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                Ok(ReleasedSweepResult::Swept {
                    metadata_patch: json!({
                        "release_sweep_status": "swept",
                        "release_sweep_terminal": true,
                        "release_sweep_attempted_at": attempted_at,
                        "release_sweep_target_address": paymaster_address,
                        "release_sweep_asset_id": asset.as_str(),
                        "release_sweep_amount": sweep_amount.to_string(),
                        "release_sweep_tx_hash": tx_hash,
                    }),
                })
            }
            AssetId::Reference(token_address) => {
                let balance = chain
                    .erc20_balance(token_address, &vault.address)
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                if balance.is_zero() {
                    return Ok(ReleasedSweepResult::Skipped {
                        metadata_patch: json!({
                            "release_sweep_status": "empty",
                            "release_sweep_terminal": true,
                            "release_sweep_attempted_at": attempted_at,
                            "release_sweep_target_address": paymaster_address,
                            "release_sweep_asset_id": asset.as_str(),
                        }),
                    });
                }

                chain
                    .ensure_native_gas_for_erc20_transfer(
                        token_address,
                        &vault.address,
                        paymaster_address,
                        balance,
                    )
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                let tx_hash = chain
                    .transfer_erc20_amount(
                        token_address,
                        wallet.private_key(),
                        paymaster_address,
                        balance,
                    )
                    .await
                    .map_err(|source| CustodyActionError::Chain { source })?;
                Ok(ReleasedSweepResult::Swept {
                    metadata_patch: json!({
                        "release_sweep_status": "swept",
                        "release_sweep_terminal": true,
                        "release_sweep_attempted_at": attempted_at,
                        "release_sweep_target_address": paymaster_address,
                        "release_sweep_asset_id": asset.as_str(),
                        "release_sweep_amount": balance.to_string(),
                        "release_sweep_tx_hash": tx_hash,
                    }),
                })
            }
        }
    }

    async fn sweep_released_bitcoin_vault(
        &self,
        vault: &CustodyVault,
        paymaster_address: &str,
        attempted_at: String,
    ) -> CustodyActionResult<ReleasedSweepResult> {
        let Some(asset) = vault.asset.as_ref() else {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "missing_asset",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                }),
            });
        };
        if !matches!(asset, AssetId::Native) {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "unsupported_asset",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                    "release_sweep_asset_id": asset.as_str(),
                }),
            });
        }

        let wallet = self.derive_wallet(vault)?;
        let Some(chain) = self.chain_registry.get_bitcoin(&ChainType::Bitcoin) else {
            return Err(CustodyActionError::ChainNotSupported {
                chain: vault.chain.clone(),
            });
        };
        let balance_sats = chain
            .address_balance_sats(&vault.address)
            .await
            .map_err(|source| CustodyActionError::Chain { source })?;
        if balance_sats == 0 {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "empty",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                    "release_sweep_asset_id": asset.as_str(),
                }),
            });
        }

        let fee_sats = chain
            .estimate_full_balance_sweep_fee_sats(&vault.address)
            .await
            .map_err(|source| CustodyActionError::Chain { source })?;
        if balance_sats <= fee_sats {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "uneconomic",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                    "release_sweep_balance": balance_sats.to_string(),
                    "release_sweep_reserved_fee": fee_sats.to_string(),
                }),
            });
        }

        let sweep_amount_sats = checked_release_sweep_sats(balance_sats, fee_sats)?;
        let raw_tx = chain
            .dump_to_address(
                &router_primitives::TokenIdentifier::Native,
                wallet.private_key(),
                paymaster_address,
                U256::from(fee_sats),
            )
            .await
            .map_err(|source| CustodyActionError::Chain { source })?;
        let tx_hash = chain
            .broadcast_signed_transaction(&raw_tx)
            .await
            .map_err(|source| CustodyActionError::Chain { source })?;
        Ok(ReleasedSweepResult::Swept {
            metadata_patch: json!({
                "release_sweep_status": "swept",
                "release_sweep_terminal": true,
                "release_sweep_attempted_at": attempted_at,
                "release_sweep_target_address": paymaster_address,
                "release_sweep_asset_id": asset.as_str(),
                "release_sweep_amount": sweep_amount_sats.to_string(),
                "release_sweep_reserved_fee": fee_sats.to_string(),
                "release_sweep_tx_hash": tx_hash,
            }),
        })
    }

    async fn sweep_released_hyperliquid_vault(
        &self,
        vault: &CustodyVault,
        paymaster_address: &str,
        attempted_at: String,
    ) -> CustodyActionResult<ReleasedSweepResult> {
        if vault.control_type == CustodyVaultControlType::HyperliquidMasterSigner {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "shared_hyperliquid_identity",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                }),
            });
        }
        let Some(runtime) = self.hyperliquid_runtime.as_ref() else {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "missing_hyperliquid_runtime",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                }),
            });
        };

        let wallet = self.derive_wallet(vault)?;
        let user = Address::from_str(&vault.address).map_err(|err| {
            CustodyActionError::InvalidHyperliquidWallet {
                reason: err.to_string(),
            }
        })?;
        let signer = wallet
            .private_key()
            .trim_start_matches("0x")
            .parse::<PrivateKeySigner>()
            .map_err(|err| CustodyActionError::InvalidHyperliquidWallet {
                reason: err.to_string(),
            })?;
        let mut info = HyperliquidInfoClient::new(runtime.base_url())
            .map_err(|source| CustodyActionError::Hyperliquid { source })?;
        info.refresh_spot_meta()
            .await
            .map_err(|source| CustodyActionError::Hyperliquid { source })?;
        let state = info
            .spot_clearinghouse_state(user)
            .await
            .map_err(|source| CustodyActionError::Hyperliquid { source })?;
        let balances: Vec<_> = state
            .balances
            .into_iter()
            .filter(|balance| decimal_string_positive(&balance.total))
            .collect();
        if balances.is_empty() {
            return Ok(ReleasedSweepResult::Skipped {
                metadata_patch: json!({
                    "release_sweep_status": "empty",
                    "release_sweep_terminal": true,
                    "release_sweep_attempted_at": attempted_at,
                    "release_sweep_target_address": paymaster_address,
                }),
            });
        }

        let client = HyperliquidExchangeClient::new(
            runtime.base_url(),
            signer,
            None,
            HyperliquidNetwork::from(runtime.network()),
        )
        .map_err(|source| CustodyActionError::Hyperliquid { source })?;
        let mut transfers = Vec::new();
        for balance in balances {
            if decimal_string_positive(&balance.hold) {
                return Ok(ReleasedSweepResult::Skipped {
                    metadata_patch: json!({
                        "release_sweep_status": "hyperliquid_balance_on_hold",
                        "release_sweep_terminal": true,
                        "release_sweep_attempted_at": attempted_at,
                        "release_sweep_target_address": paymaster_address,
                        "release_sweep_coin": balance.coin,
                    }),
                });
            }
            let token = info
                .spot_token_wire(&balance.coin)
                .map_err(|source| CustodyActionError::Hyperliquid { source })?;
            let time_ms = current_hyperliquid_nonce_time_ms("release sweep send_asset")?;
            let response = client
                .send_asset(
                    paymaster_address.to_string(),
                    "spot".to_string(),
                    "spot".to_string(),
                    token.clone(),
                    balance.total.clone(),
                    time_ms,
                )
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?;
            if let Some(reason) = hyperliquid_action_error(&response) {
                return Err(CustodyActionError::HyperliquidAction { reason });
            }
            transfers.push(json!({
                "coin": balance.coin,
                "token": token,
                "amount": balance.total,
                "response": response,
            }));
        }

        Ok(ReleasedSweepResult::Swept {
            metadata_patch: json!({
                "release_sweep_status": "swept",
                "release_sweep_terminal": true,
                "release_sweep_attempted_at": attempted_at,
                "release_sweep_target_address": paymaster_address,
                "release_sweep_transfers": transfers,
            }),
        })
    }
}

fn validate_derived_address(
    vault: &CustodyVault,
    derived_address: &str,
) -> CustodyActionResult<()> {
    let matches = if vault.chain.evm_chain_id().is_some() {
        match (
            Address::from_str(derived_address),
            Address::from_str(&vault.address),
        ) {
            (Ok(derived), Ok(stored)) => derived == stored,
            _ => false,
        }
    } else {
        derived_address == vault.address
    };
    if matches {
        return Ok(());
    }

    Err(CustodyActionError::CustodyAddressMismatch {
        vault_id: vault.id,
        derived_address: derived_address.to_string(),
        vault_address: vault.address.clone(),
    })
}

fn json_object_or_wrapped(value: Value) -> Value {
    if value.is_object() {
        value
    } else {
        json!({ "value": value })
    }
}

fn decimal_string_positive(value: &str) -> bool {
    value.chars().any(|ch| ch.is_ascii_digit() && ch != '0')
}

fn current_hyperliquid_nonce_time_ms(context: &'static str) -> CustodyActionResult<u64> {
    hyperliquid_nonce_time_ms_from_timestamp(chrono::Utc::now().timestamp_millis(), context)
}

fn hyperliquid_nonce_time_ms_from_timestamp(
    timestamp_millis: i64,
    context: &'static str,
) -> CustodyActionResult<u64> {
    u64::try_from(timestamp_millis).map_err(|_| CustodyActionError::InvalidHyperliquidTimestamp {
        context,
        reason: "system clock is before Unix epoch".to_string(),
    })
}

pub fn evm_address_from_private_key(private_key: &str) -> CustodyActionResult<String> {
    let signer = private_key
        .trim_start_matches("0x")
        .parse::<PrivateKeySigner>()
        .map_err(|err| CustodyActionError::InvalidHyperliquidWallet {
            reason: err.to_string(),
        })?;
    Ok(format!("{:#x}", signer.address()))
}

pub fn bitcoin_address_from_private_key(
    private_key: &str,
    network: bitcoin::Network,
) -> CustodyActionResult<String> {
    let private_key =
        PrivateKey::from_wif(private_key).map_err(|err| CustodyActionError::InvalidAmount {
            field: "bitcoin_paymaster_private_key",
            reason: err.to_string(),
        })?;
    let public_key = CompressedPublicKey::from_private_key(&Secp256k1::new(), &private_key)
        .map_err(|err| CustodyActionError::InvalidAmount {
            field: "bitcoin_paymaster_private_key",
            reason: err.to_string(),
        })?;
    Ok(bitcoin::Address::p2wpkh(&public_key, network).to_string())
}

fn parse_positive_u256(field: &'static str, value: &str) -> CustodyActionResult<U256> {
    let amount = parse_u256(field, value)?;
    if amount.is_zero() {
        return Err(CustodyActionError::InvalidAmount {
            field,
            reason: "amount must be greater than zero".to_string(),
        });
    }
    Ok(amount)
}

fn parse_u256(field: &'static str, value: &str) -> CustodyActionResult<U256> {
    U256::from_str_radix(value, 10).map_err(|err| CustodyActionError::InvalidAmount {
        field,
        reason: err.to_string(),
    })
}

/// Sign and submit a Hyperliquid action using the custody vault's derived EVM
/// key. Returns `(tx_hash, response_body)`. HL order actions expose a local
/// action hash through the client response; when absent, we use a synthetic
/// fallback so upstream receipt indexing still has a stable handle. The full
/// response is forwarded so the provider's `post_execute` can derive richer
/// observed state.
async fn execute_hyperliquid_call(
    call: &HyperliquidCall,
    private_key: &str,
) -> CustodyActionResult<(String, Value)> {
    let pk_hex = private_key.trim_start_matches("0x");
    let wallet = pk_hex.parse::<PrivateKeySigner>().map_err(|err| {
        CustodyActionError::InvalidHyperliquidWallet {
            reason: err.to_string(),
        }
    })?;
    let vault_address = call
        .vault_address
        .as_deref()
        .map(|raw| {
            Address::from_str(raw).map_err(|err| {
                CustodyActionError::InvalidHyperliquidVaultAddress {
                    reason: err.to_string(),
                }
            })
        })
        .transpose()?;

    let client = HyperliquidExchangeClient::new(
        &call.target_base_url,
        wallet,
        vault_address,
        HyperliquidNetwork::from(call.network),
    )
    .map_err(|source| CustodyActionError::Hyperliquid { source })?;

    let response = match &call.payload {
        HyperliquidCallPayload::L1Action { action } => match action {
            HyperliquidActions::Order(bulk) => client
                .place_orders(bulk.orders.clone(), &bulk.grouping)
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?,
            HyperliquidActions::Cancel(bulk) => client
                .cancel_orders(bulk.cancels.clone())
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?,
            HyperliquidActions::ScheduleCancel(schedule) => client
                .schedule_cancel(schedule.time)
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?,
        },
        HyperliquidCallPayload::UsdClassTransfer { amount, to_perp } => {
            let time_ms = current_hyperliquid_nonce_time_ms("usd_class_transfer")?;
            client
                .usd_class_transfer(amount.clone(), *to_perp, time_ms)
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?
        }
        HyperliquidCallPayload::Withdraw3 {
            destination,
            amount,
        } => {
            let time_ms = current_hyperliquid_nonce_time_ms("withdraw3")?;
            client
                .withdraw_to_bridge(destination.clone(), amount.clone(), time_ms)
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?
        }
        HyperliquidCallPayload::SpotSend {
            destination,
            token,
            amount,
        } => {
            let time_ms = current_hyperliquid_nonce_time_ms("spot_send")?;
            client
                .spot_send(destination.clone(), token.clone(), amount.clone(), time_ms)
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?
        }
        HyperliquidCallPayload::SendAsset {
            destination,
            source_dex,
            destination_dex,
            token,
            amount,
        } => {
            let time_ms = current_hyperliquid_nonce_time_ms("send_asset")?;
            client
                .send_asset(
                    destination.clone(),
                    source_dex.clone(),
                    destination_dex.clone(),
                    token.clone(),
                    amount.clone(),
                    time_ms,
                )
                .await
                .map_err(|source| CustodyActionError::Hyperliquid { source })?
        }
    };

    if let Some(reason) = hyperliquid_action_error(&response) {
        return Err(CustodyActionError::HyperliquidAction { reason });
    }

    let tx_hash = synthesize_hyperliquid_tx_hash(&call.payload, &response)?;
    Ok((tx_hash, response))
}

fn hyperliquid_action_error(response: &Value) -> Option<String> {
    match response.get("status").and_then(Value::as_str) {
        Some(status) if status.eq_ignore_ascii_case("ok") => {}
        Some(_) => {
            return Some(
                response
                    .get("response")
                    .map(Value::to_string)
                    .unwrap_or_else(|| response.to_string()),
            );
        }
        None => {
            return Some(format!(
                "hyperliquid action response missing status: {response}"
            ));
        }
    }

    let statuses = response
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)?;
    statuses
        .iter()
        .find_map(|status| status.get("error").map(Value::to_string))
}

/// HL's `/exchange` replies with a JSON envelope; our client annotates L1
/// action responses with the Hyperliquid action hash. Prefer that real
/// explorer hash, then fall back to a stable hash-shaped handle derived from
/// the payload and response.
fn synthesize_hyperliquid_tx_hash(
    payload: &HyperliquidCallPayload,
    response: &Value,
) -> CustodyActionResult<String> {
    if let Some(hash) = response.get("hash").and_then(Value::as_str) {
        return Ok(hash.to_string());
    }

    // `{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":123}}]}}}`
    if let Some(statuses) = response
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
    {
        for status in statuses {
            if let Some(oid) = status
                .pointer("/resting/oid")
                .or_else(|| status.pointer("/filled/oid"))
                .and_then(Value::as_u64)
            {
                return synthetic_hyperliquid_response_hash(
                    payload,
                    response,
                    &format!("oid:{oid}"),
                );
            }
        }
    }

    match payload {
        HyperliquidCallPayload::UsdClassTransfer { .. } => {
            synthetic_hyperliquid_response_hash(payload, response, "usd_class_transfer")
        }
        HyperliquidCallPayload::Withdraw3 { .. } => {
            synthetic_hyperliquid_response_hash(payload, response, "withdraw3")
        }
        HyperliquidCallPayload::SpotSend { .. } => {
            synthetic_hyperliquid_response_hash(payload, response, "spot_send")
        }
        HyperliquidCallPayload::SendAsset { .. } => {
            synthetic_hyperliquid_response_hash(payload, response, "send_asset")
        }
        HyperliquidCallPayload::L1Action { action } => match action {
            HyperliquidActions::ScheduleCancel(_) => {
                synthetic_hyperliquid_response_hash(payload, response, "schedule_cancel")
            }
            HyperliquidActions::Order(_) => {
                synthetic_hyperliquid_response_hash(payload, response, "l1_order")
            }
            HyperliquidActions::Cancel(_) => {
                synthetic_hyperliquid_response_hash(payload, response, "l1_cancel")
            }
        },
    }
}

fn synthetic_hyperliquid_response_hash(
    payload: &HyperliquidCallPayload,
    response: &Value,
    discriminator: &str,
) -> CustodyActionResult<String> {
    let payload_bytes =
        serde_json::to_vec(payload).map_err(|error| CustodyActionError::Serialization {
            reason: format!("hyperliquid payload could not be encoded: {error}"),
        })?;
    let response_bytes =
        serde_json::to_vec(response).map_err(|error| CustodyActionError::Serialization {
            reason: format!("hyperliquid response could not be encoded: {error}"),
        })?;
    let mut preimage = Vec::with_capacity(
        b"rift-hyperliquid-response-v1".len()
            + discriminator.len()
            + payload_bytes.len()
            + response_bytes.len()
            + 4,
    );
    preimage.extend_from_slice(b"rift-hyperliquid-response-v1");
    preimage.push(b'|');
    preimage.extend_from_slice(discriminator.as_bytes());
    preimage.push(b'|');
    preimage.extend_from_slice(&payload_bytes);
    preimage.push(b'|');
    preimage.extend_from_slice(&response_bytes);
    Ok(format!("{:#x}", keccak256(preimage)))
}

fn decode_calldata(value: &str) -> CustodyActionResult<Bytes> {
    let stripped = value.strip_prefix("0x").unwrap_or(value);
    if !stripped.len().is_multiple_of(2) {
        return Err(CustodyActionError::InvalidCalldata {
            reason: "hex calldata must have an even number of characters".to_string(),
        });
    }
    let bytes =
        alloy::hex::decode(stripped).map_err(|err| CustodyActionError::InvalidCalldata {
            reason: err.to_string(),
        })?;
    Ok(Bytes::from(bytes))
}

fn checked_release_sweep_amount(balance: U256, reserved_fee: U256) -> CustodyActionResult<U256> {
    balance
        .checked_sub(reserved_fee)
        .ok_or_else(|| CustodyActionError::InvalidAmount {
            field: "release_sweep_amount",
            reason: "balance minus reserved fee underflowed".to_string(),
        })
}

fn checked_release_sweep_sats(balance_sats: u64, fee_sats: u64) -> CustodyActionResult<u64> {
    balance_sats
        .checked_sub(fee_sats)
        .ok_or_else(|| CustodyActionError::InvalidAmount {
            field: "release_sweep_amount",
            reason: "bitcoin balance minus reserved fee underflowed".to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn withdraw_payload() -> HyperliquidCallPayload {
        HyperliquidCallPayload::Withdraw3 {
            destination: "0x1111111111111111111111111111111111111111".to_string(),
            amount: "1.25".to_string(),
        }
    }

    #[test]
    fn hyperliquid_execution_config_debug_redacts_private_key() {
        let private_key = "0x59c6995e998f97a5a0044966f094538b292b05a59f7807f5d8f6ab1234d1f001";
        let config = HyperliquidExecutionConfig::new(
            private_key,
            Some(Address::repeat_byte(0x11)),
            Some(Address::repeat_byte(0x22)),
        )
        .expect("valid config");

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("59c6995e998f97a5a0044966f094538b"));
        assert!(rendered.contains("redacted"));
        assert!(rendered.contains("1111111111111111111111111111111111111111"));
        assert!(rendered.contains("2222222222222222222222222222222222222222"));
    }

    #[test]
    fn hyperliquid_tx_hash_prefers_exchange_action_hash() {
        let response = json!({
            "status": "ok",
            "hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "response": { "type": "order" }
        });

        assert_eq!(
            synthesize_hyperliquid_tx_hash(&withdraw_payload(), &response).unwrap(),
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn hyperliquid_tx_hash_fallback_is_hash_shaped_not_oid_handle() {
        let response = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{ "filled": { "oid": 1109 } }]
                }
            }
        });

        let tx_hash = synthesize_hyperliquid_tx_hash(&withdraw_payload(), &response).unwrap();

        assert!(tx_hash.starts_with("0x"));
        assert_eq!(tx_hash.len(), 66);
        assert!(!tx_hash.starts_with("hl:oid:"));
    }

    #[test]
    fn hyperliquid_tx_hash_fallback_changes_with_response_identity() {
        let first = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{ "filled": { "oid": 1109 } }]
                }
            }
        });
        let second = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{ "filled": { "oid": 1110 } }]
                }
            }
        });

        assert_ne!(
            synthesize_hyperliquid_tx_hash(&withdraw_payload(), &first).unwrap(),
            synthesize_hyperliquid_tx_hash(&withdraw_payload(), &second).unwrap()
        );
    }

    #[test]
    fn hyperliquid_action_error_rejects_missing_status() {
        let response = json!({
            "response": {
                "type": "order"
            }
        });

        let error = hyperliquid_action_error(&response).expect("missing status");

        assert!(error.contains("missing status"), "{error}");
    }

    #[test]
    fn hyperliquid_action_error_rejects_non_ok_status() {
        let response = json!({
            "status": "error",
            "response": "bad action"
        });

        let error = hyperliquid_action_error(&response).expect("non-ok status");

        assert!(error.contains("bad action"), "{error}");
    }

    #[test]
    fn hyperliquid_action_error_accepts_ok_without_status_errors() {
        let response = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{ "resting": { "oid": 1109 } }]
                }
            }
        });

        assert!(hyperliquid_action_error(&response).is_none());
    }

    #[test]
    fn hyperliquid_action_error_rejects_status_entry_errors() {
        let response = json!({
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [{ "error": "insufficient margin" }]
                }
            }
        });

        let error = hyperliquid_action_error(&response).expect("status entry error");

        assert!(error.contains("insufficient margin"), "{error}");
    }

    #[test]
    fn hyperliquid_nonce_time_rejects_pre_epoch_timestamp() {
        let error = hyperliquid_nonce_time_ms_from_timestamp(-1, "test nonce")
            .expect_err("negative timestamps must reject");

        assert!(matches!(
            error,
            CustodyActionError::InvalidHyperliquidTimestamp { context, .. }
                if context == "test nonce"
        ));
    }

    #[test]
    fn hyperliquid_nonce_time_accepts_unix_epoch_boundary() {
        assert_eq!(
            hyperliquid_nonce_time_ms_from_timestamp(0, "test nonce").unwrap(),
            0
        );
    }

    #[test]
    fn release_sweep_amounts_reject_underflow() {
        assert_eq!(
            checked_release_sweep_amount(U256::from(10_u64), U256::from(3_u64))
                .unwrap()
                .to_string(),
            "7"
        );
        assert!(checked_release_sweep_amount(U256::from(3_u64), U256::from(10_u64)).is_err());
        assert_eq!(checked_release_sweep_sats(10, 3).unwrap(), 7);
        assert!(checked_release_sweep_sats(3, 10).is_err());
    }
}
