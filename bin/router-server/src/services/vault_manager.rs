use crate::{
    api::CreateVaultRequest,
    config::Settings,
    db::Database,
    error::RouterServerError,
    models::{
        DepositVault, DepositVaultFundingHint, DepositVaultStatus, ProviderOperationHintStatus,
        RouterOrderStatus, VaultAction,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId, DepositAsset},
    services::deposit_address::{derive_deposit_address_for_quote, DepositAddressError},
    telemetry,
};
use alloy::primitives::{keccak256, U256};
use blockchain_utils::MempoolEsploraFeeExt;
use chains::ChainRegistry;
use chrono::{Duration, Utc};
use router_primitives::{ChainType, TokenIdentifier};
use serde_json::json;
use snafu::Snafu;
use std::{sync::Arc, time::Instant};
use tracing::{debug, info, warn};
use uuid::Uuid;

const CANCELLATION_COMMITMENT_DOMAIN: &[u8] = b"router-server-cancel-v1";
const DEFAULT_CANCEL_AFTER: Duration = Duration::hours(24);
const FUNDING_PASS_LIMIT: i64 = 100;
const REFUND_PASS_LIMIT: i64 = 100;
const REFUND_LEASE_DURATION: Duration = Duration::minutes(5);
const REFUND_RETRY_DELAY: Duration = Duration::seconds(30);

#[derive(Debug, Snafu)]
pub enum VaultError {
    #[snafu(display("Chain not supported: {}", chain))]
    ChainNotSupported { chain: ChainId },

    #[snafu(display("Invalid asset id {} for {}: {}", asset, chain, reason))]
    InvalidAssetId {
        asset: String,
        chain: ChainId,
        reason: String,
    },

    #[snafu(display("Invalid recovery address {} for {}", address, chain))]
    InvalidRecoveryAddress { address: String, chain: ChainId },

    #[snafu(display("Invalid metadata: {}", reason))]
    InvalidMetadata { reason: String },

    #[snafu(display("Invalid cancellation commitment: {}", reason))]
    InvalidCancellationCommitment { reason: String },

    #[snafu(display("Invalid cancellation secret"))]
    InvalidCancellationSecret,

    #[snafu(display("Refund not allowed: {}", reason))]
    RefundNotAllowed { reason: String },

    #[snafu(display("Invalid order binding: {}", reason))]
    InvalidOrderBinding { reason: String },

    #[snafu(display("Invalid funding amount {}: {}", field, reason))]
    InvalidFundingAmount { field: &'static str, reason: String },

    #[snafu(display("Funding check failed: {}", message))]
    FundingCheck { message: String },

    #[snafu(display("Failed to generate vault salt: {}", source))]
    Random { source: Box<getrandom::Error> },

    #[snafu(display("Failed to derive wallet: {}", source))]
    WalletDerivation { source: Box<chains::Error> },

    #[snafu(display("Deposit address derivation failed: {}", source))]
    DepositAddress { source: Box<DepositAddressError> },

    #[snafu(display("Database error: {}", source))]
    Database { source: Box<RouterServerError> },
}

pub type VaultResult<T> = Result<T, VaultError>;

impl VaultError {
    fn random(source: getrandom::Error) -> Self {
        Self::Random {
            source: Box::new(source),
        }
    }

    fn wallet_derivation(source: chains::Error) -> Self {
        Self::WalletDerivation {
            source: Box::new(source),
        }
    }

    fn deposit_address(source: DepositAddressError) -> Self {
        Self::DepositAddress {
            source: Box::new(source),
        }
    }

    fn database(source: RouterServerError) -> Self {
        Self::Database {
            source: Box::new(source),
        }
    }
}

struct OrderBinding {
    action: VaultAction,
    quote_id: Uuid,
}

#[derive(Debug, Clone)]
enum FundingHintDisposition {
    Funded { vault: Box<DepositVault> },
    Ignored { reason: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FundingHintPassSummary {
    pub processed: usize,
    pub funded_order_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefundPassSummary {
    pub timeout_claimed: usize,
    pub retry_claimed: usize,
    pub refunded_order_ids: Vec<Uuid>,
}

pub struct VaultManager {
    db: Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    worker_id: String,
}

impl VaultManager {
    #[must_use]
    pub fn new(db: Database, settings: Arc<Settings>, chain_registry: Arc<ChainRegistry>) -> Self {
        Self::with_worker_id(
            db,
            settings,
            chain_registry,
            format!("router-server-{}", Uuid::now_v7()),
        )
    }

    #[must_use]
    pub fn with_worker_id(
        db: Database,
        settings: Arc<Settings>,
        chain_registry: Arc<ChainRegistry>,
        worker_id: String,
    ) -> Self {
        Self {
            db,
            settings,
            chain_registry,
            worker_id,
        }
    }

    pub async fn create_vault(&self, request: CreateVaultRequest) -> VaultResult<DepositVault> {
        Self::validate_metadata(&request.metadata)?;
        let deposit_asset = self.validate_and_normalize_deposit_asset(&request.deposit_asset)?;
        let order_binding = if let Some(order_id) = request.order_id {
            Some(
                self.validate_order_binding(order_id, &deposit_asset, &request.action)
                    .await?,
            )
        } else {
            None
        };
        let recovery_address = self.validate_and_normalize_recovery_address(
            &deposit_asset.chain,
            &request.recovery_address,
        )?;
        let cancellation_commitment = normalize_hex_32(&request.cancellation_commitment)
            .map_err(|reason| VaultError::InvalidCancellationCommitment { reason })?;

        let cancel_after = request
            .cancel_after
            .unwrap_or_else(|| Utc::now() + DEFAULT_CANCEL_AFTER);
        if cancel_after <= Utc::now() {
            return Err(VaultError::InvalidCancellationCommitment {
                reason: "cancel_after must be in the future".to_string(),
            });
        }

        let (deposit_vault_salt, deposit_vault_address) = match &order_binding {
            Some(binding) => {
                let (address, salt) = derive_deposit_address_for_quote(
                    self.chain_registry.as_ref(),
                    &self.settings.master_key_bytes(),
                    binding.quote_id,
                    &deposit_asset.chain,
                )
                .map_err(VaultError::deposit_address)?;
                (salt, address)
            }
            None => {
                let backend_chain = backend_chain_for_id(&deposit_asset.chain).ok_or(
                    VaultError::ChainNotSupported {
                        chain: deposit_asset.chain.clone(),
                    },
                )?;
                let chain = self.chain_registry.get(&backend_chain).ok_or(
                    VaultError::ChainNotSupported {
                        chain: deposit_asset.chain.clone(),
                    },
                )?;
                let mut salt = [0u8; 32];
                getrandom::getrandom(&mut salt).map_err(VaultError::random)?;
                let wallet = chain
                    .derive_wallet(&self.settings.master_key_bytes(), &salt)
                    .map_err(VaultError::wallet_derivation)?;
                (salt, wallet.address.clone())
            }
        };

        let now = Utc::now();
        let vault = DepositVault {
            id: Uuid::now_v7(),
            order_id: request.order_id,
            deposit_asset,
            action: order_binding
                .map(|binding| binding.action)
                .unwrap_or(request.action),
            metadata: request.metadata,
            deposit_vault_salt,
            deposit_vault_address,
            recovery_address,
            cancellation_commitment,
            cancel_after,
            status: DepositVaultStatus::PendingFunding,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            created_at: now,
            updated_at: now,
        };

        if let Some(order_id) = vault.order_id {
            self.db
                .vaults()
                .create_and_attach_order(&vault, order_id, now)
                .await
                .map_err(map_create_vault_error)?;
        } else {
            self.db
                .vaults()
                .create(&vault)
                .await
                .map_err(VaultError::database)?;
        }

        telemetry::record_vault_created(&vault);
        info!(
            vault_id = %vault.id,
            chain = %vault.deposit_asset.chain,
            asset_kind = if vault.deposit_asset.asset.is_native() { "native" } else { "reference" },
            status = vault.status.to_db_string(),
            "Router-server vault created"
        );

        Ok(vault)
    }

    pub async fn get_vault(&self, id: Uuid) -> VaultResult<DepositVault> {
        self.db.vaults().get(id).await.map_err(VaultError::database)
    }

    pub async fn cancel_vault(
        &self,
        id: Uuid,
        cancellation_secret: &str,
    ) -> VaultResult<DepositVault> {
        let vault = self.get_vault(id).await?;
        telemetry::record_vault_cancel_requested(&vault);
        self.verify_cancellation_secret(cancellation_secret, &vault.cancellation_commitment)?;
        self.ensure_refund_allowed(&vault)?;

        let now = Utc::now();
        let requested_vault = self
            .db
            .vaults()
            .request_refund(id, now)
            .await
            .map_err(VaultError::database)?;
        if requested_vault.status != vault.status {
            telemetry::record_vault_transition(
                &requested_vault,
                vault.status,
                requested_vault.status,
            );
        }

        Ok(requested_vault)
    }

    pub async fn process_funding_pass(&self) -> VaultResult<usize> {
        let vaults = self
            .db
            .vaults()
            .get_pending_funding(FUNDING_PASS_LIMIT)
            .await
            .map_err(VaultError::database)?;
        let mut funded = 0_usize;

        for vault in vaults {
            match self.is_vault_funded(&vault).await {
                Ok(true) => {
                    let funded_vault = match self
                        .db
                        .vaults()
                        .transition_status(
                            vault.id,
                            DepositVaultStatus::PendingFunding,
                            DepositVaultStatus::Funded,
                            Utc::now(),
                        )
                        .await
                    {
                        Ok(funded_vault) => funded_vault,
                        Err(RouterServerError::NotFound) => {
                            debug!(
                                vault_id = %vault.id,
                                "Vault funding transition was already claimed"
                            );
                            continue;
                        }
                        Err(source) => return Err(VaultError::database(source)),
                    };
                    telemetry::record_vault_transition(
                        &funded_vault,
                        DepositVaultStatus::PendingFunding,
                        DepositVaultStatus::Funded,
                    );
                    info!(
                        vault_id = %funded_vault.id,
                        chain = %funded_vault.deposit_asset.chain,
                        asset_kind = if funded_vault.deposit_asset.asset.is_native() { "native" } else { "reference" },
                        "Router-server vault funding detected"
                    );
                    funded += 1;
                }
                Ok(false) => {}
                Err(err) => {
                    warn!(vault_id = %vault.id, error = %err, "Funding check failed");
                }
            }
        }

        Ok(funded)
    }

    pub async fn record_funding_hint(
        &self,
        hint: DepositVaultFundingHint,
    ) -> VaultResult<DepositVaultFundingHint> {
        self.db
            .vaults()
            .get(hint.vault_id)
            .await
            .map_err(VaultError::database)?;
        self.db
            .vaults()
            .create_funding_hint(&hint)
            .await
            .map_err(VaultError::database)
    }

    pub async fn process_funding_hints(&self, limit: i64) -> VaultResult<usize> {
        Ok(self.process_funding_hints_detailed(limit).await?.processed)
    }

    pub async fn process_funding_hints_detailed(
        &self,
        limit: i64,
    ) -> VaultResult<FundingHintPassSummary> {
        let hints = self
            .db
            .vaults()
            .claim_pending_funding_hints(limit, Utc::now())
            .await
            .map_err(VaultError::database)?;
        let mut summary = FundingHintPassSummary::default();

        for hint in hints {
            match self.process_funding_hint(&hint).await {
                Ok(FundingHintDisposition::Funded { vault }) => {
                    self.db
                        .vaults()
                        .complete_funding_hint(
                            hint.id,
                            ProviderOperationHintStatus::Processed,
                            json!({}),
                            Utc::now(),
                        )
                        .await
                        .map_err(VaultError::database)?;
                    summary.processed += 1;
                    if let Some(order_id) = vault.order_id {
                        summary.funded_order_ids.push(order_id);
                    }
                }
                Ok(FundingHintDisposition::Ignored { reason }) => {
                    self.db
                        .vaults()
                        .complete_funding_hint(
                            hint.id,
                            ProviderOperationHintStatus::Ignored,
                            json!({ "reason": reason }),
                            Utc::now(),
                        )
                        .await
                        .map_err(VaultError::database)?;
                    summary.processed += 1;
                }
                Err(error) => {
                    self.db
                        .vaults()
                        .complete_funding_hint(
                            hint.id,
                            ProviderOperationHintStatus::Failed,
                            json!({ "error": error.to_string() }),
                            Utc::now(),
                        )
                        .await
                        .map_err(VaultError::database)?;
                    summary.processed += 1;
                }
            }
        }

        Ok(summary)
    }

    async fn process_funding_hint(
        &self,
        hint: &DepositVaultFundingHint,
    ) -> VaultResult<FundingHintDisposition> {
        let vault = self
            .db
            .vaults()
            .get(hint.vault_id)
            .await
            .map_err(VaultError::database)?;
        match vault.status {
            DepositVaultStatus::PendingFunding => {}
            DepositVaultStatus::Funded => {
                return Ok(FundingHintDisposition::Ignored {
                    reason: "vault already funded".to_string(),
                });
            }
            status => {
                return Ok(FundingHintDisposition::Ignored {
                    reason: format!("vault status is {}", status.to_db_string()),
                });
            }
        }

        if !self.is_vault_funded(&vault).await? {
            return Ok(FundingHintDisposition::Ignored {
                reason: "vault balance is still below required funding amount".to_string(),
            });
        }

        let funded_vault = match self
            .db
            .vaults()
            .transition_status(
                vault.id,
                DepositVaultStatus::PendingFunding,
                DepositVaultStatus::Funded,
                Utc::now(),
            )
            .await
        {
            Ok(funded_vault) => funded_vault,
            Err(RouterServerError::NotFound) => {
                return Ok(FundingHintDisposition::Ignored {
                    reason: "vault funding transition was already claimed".to_string(),
                });
            }
            Err(source) => return Err(VaultError::database(source)),
        };
        telemetry::record_vault_transition(
            &funded_vault,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
        );
        info!(
            vault_id = %funded_vault.id,
            hint_id = %hint.id,
            chain = %funded_vault.deposit_asset.chain,
            asset_kind = if funded_vault.deposit_asset.asset.is_native() { "native" } else { "reference" },
            "Router worker validated vault funding hint"
        );

        Ok(FundingHintDisposition::Funded {
            vault: Box::new(funded_vault),
        })
    }

    pub async fn process_refund_pass(&self) -> RefundPassSummary {
        let now = Utc::now();
        let mut summary = RefundPassSummary::default();

        let timeout_pass_started = Instant::now();
        match self
            .db
            .vaults()
            .claim_due_for_timeout(
                now,
                now + REFUND_LEASE_DURATION,
                &self.worker_id,
                REFUND_PASS_LIMIT,
            )
            .await
        {
            Ok(vaults) => {
                telemetry::record_refund_pass(
                    "timeout",
                    vaults.len(),
                    timeout_pass_started.elapsed(),
                );
                summary.timeout_claimed = vaults.len();
                for vault in vaults {
                    match self.attempt_refund(vault.clone()).await {
                        Ok(refunded_vault) => {
                            if refunded_vault.status == DepositVaultStatus::Refunded {
                                if let Some(order_id) = refunded_vault.order_id {
                                    summary.refunded_order_ids.push(order_id);
                                }
                            }
                        }
                        Err(err) => {
                            warn!(vault_id = %vault.id, error = %err, "Timeout-triggered refund attempt failed");
                        }
                    }
                }
            }
            Err(err) => {
                telemetry::record_refund_pass("timeout", 0, timeout_pass_started.elapsed());
                warn!(error = %err, "Failed to fetch due router-server timeouts");
            }
        }

        let retry_pass_started_at = Utc::now();
        let retry_pass_started = Instant::now();
        match self
            .db
            .vaults()
            .claim_refunding(
                retry_pass_started_at,
                retry_pass_started_at + REFUND_LEASE_DURATION,
                &self.worker_id,
                REFUND_PASS_LIMIT,
            )
            .await
        {
            Ok(vaults) => {
                telemetry::record_refund_pass("retry", vaults.len(), retry_pass_started.elapsed());
                summary.retry_claimed = vaults.len();
                for vault in vaults {
                    let vault_id = vault.id;
                    match self.attempt_refund(vault).await {
                        Ok(refunded_vault) => {
                            if refunded_vault.status == DepositVaultStatus::Refunded {
                                if let Some(order_id) = refunded_vault.order_id {
                                    summary.refunded_order_ids.push(order_id);
                                }
                            }
                        }
                        Err(err) => {
                            warn!(vault_id = %vault_id, error = %err, "Refund retry pass failed");
                        }
                    }
                }
            }
            Err(err) => {
                telemetry::record_refund_pass("retry", 0, retry_pass_started.elapsed());
                warn!(error = %err, "Failed to fetch refunding router-server vaults");
            }
        }

        summary
    }

    async fn attempt_refund(&self, vault: DepositVault) -> VaultResult<DepositVault> {
        let refund_started = Instant::now();
        if vault.status == DepositVaultStatus::Refunded {
            return Ok(vault);
        }
        if vault.status != DepositVaultStatus::Refunding {
            telemetry::record_refund_failure(
                &vault,
                "refund_not_allowed",
                refund_started.elapsed(),
            );
            return Err(VaultError::RefundNotAllowed {
                reason: format!(
                    "vault status {} cannot be refunded",
                    vault.status.to_db_string()
                ),
            });
        }
        telemetry::record_refund_attempt(&vault);

        let backend_chain = backend_chain_for_id(&vault.deposit_asset.chain).ok_or(
            VaultError::ChainNotSupported {
                chain: vault.deposit_asset.chain.clone(),
            },
        )?;
        let chain =
            self.chain_registry
                .get(&backend_chain)
                .ok_or(VaultError::ChainNotSupported {
                    chain: vault.deposit_asset.chain.clone(),
                })?;
        let deposit_wallet = chain
            .derive_wallet(&self.settings.master_key_bytes(), &vault.deposit_vault_salt)
            .map_err(VaultError::wallet_derivation)?;

        let refund_result = match backend_chain {
            ChainType::Bitcoin => {
                self.refund_bitcoin(&vault, deposit_wallet.private_key())
                    .await
            }
            ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base => {
                self.refund_evm(&vault, deposit_wallet.private_key()).await
            }
            ChainType::Hyperliquid => {
                Err("hyperliquid vaults are never user-funded so have no refund path".to_string())
            }
        };

        match refund_result {
            Ok(tx_hash) => {
                info!(vault_id = %vault.id, %tx_hash, "Router-server vault refunded");
                let refunded_vault = self
                    .db
                    .vaults()
                    .mark_refunded(vault.id, Utc::now(), &tx_hash)
                    .await
                    .map_err(VaultError::database)?;
                telemetry::record_refund_success(&refunded_vault, refund_started.elapsed());
                telemetry::record_vault_transition(
                    &refunded_vault,
                    vault.status,
                    refunded_vault.status,
                );

                Ok(refunded_vault)
            }
            Err(err) => {
                let message = err;
                warn!(vault_id = %vault.id, error = %message, "Router-server refund attempt did not complete");
                telemetry::record_refund_failure(&vault, "chain_error", refund_started.elapsed());
                let failed_at = Utc::now();
                self.db
                    .vaults()
                    .mark_refund_error(
                        vault.id,
                        failed_at,
                        failed_at + REFUND_RETRY_DELAY,
                        &message,
                    )
                    .await
                    .map_err(VaultError::database)
            }
        }
    }

    async fn refund_bitcoin(
        &self,
        vault: &DepositVault,
        private_key: &str,
    ) -> Result<String, String> {
        let bitcoin_chain = self
            .chain_registry
            .get_bitcoin(
                &backend_chain_for_id(&vault.deposit_asset.chain)
                    .ok_or_else(|| "Bitcoin chain is not configured".to_string())?,
            )
            .ok_or_else(|| "Bitcoin chain is not configured".to_string())?;
        let balance = bitcoin_chain
            .address_balance_sats(&vault.deposit_vault_address)
            .await
            .map_err(|err| err.to_string())?;
        if balance == 0 {
            return Err("No bitcoin funds found yet for refund".to_string());
        }

        let chain = self
            .chain_registry
            .get(
                &backend_chain_for_id(&vault.deposit_asset.chain)
                    .ok_or_else(|| "Bitcoin chain is not configured".to_string())?,
            )
            .ok_or_else(|| "Bitcoin chain is not configured".to_string())?;
        let esplora = chain
            .esplora_client()
            .ok_or_else(|| "Esplora client not available for bitcoin refund".to_string())?;
        let next_block_fee_rate = esplora
            .get_mempool_fee_estimate_next_block()
            .await
            .map_err(|err| format!("Failed to query bitcoin fee estimate: {err}"))?;
        let fee = (next_block_fee_rate * 125.0).ceil() as u64;

        let tx_data = chain
            .dump_to_address(
                &token_identifier(&vault.deposit_asset.asset),
                private_key,
                &vault.recovery_address,
                U256::from(fee),
            )
            .await
            .map_err(|err| err.to_string())?;
        bitcoin_chain
            .broadcast_signed_transaction(&tx_data)
            .await
            .map_err(|err| err.to_string())
    }

    async fn refund_evm(&self, vault: &DepositVault, private_key: &str) -> Result<String, String> {
        let evm_chain = self
            .chain_registry
            .get_evm(
                &backend_chain_for_id(&vault.deposit_asset.chain)
                    .ok_or_else(|| "EVM chain is not configured".to_string())?,
            )
            .ok_or_else(|| "EVM chain is not configured".to_string())?;

        let balance = match &vault.deposit_asset.asset {
            AssetId::Native => evm_chain
                .native_balance(&vault.deposit_vault_address)
                .await
                .map_err(|err| err.to_string())?,
            AssetId::Reference(token) => evm_chain
                .erc20_balance(token, &vault.deposit_vault_address)
                .await
                .map_err(|err| err.to_string())?,
        };
        if balance == U256::ZERO {
            return Err("No EVM funds found yet for refund".to_string());
        }

        if let AssetId::Reference(token) = &vault.deposit_asset.asset {
            match evm_chain
                .ensure_native_gas_for_erc20_refund(
                    token,
                    &vault.deposit_vault_address,
                    &vault.recovery_address,
                    balance,
                )
                .await
            {
                Ok(Some(tx_hash)) => {
                    info!(vault_id = %vault.id, %tx_hash, "Topped up EVM vault gas before refund");
                }
                Ok(None) => {}
                Err(err) => {
                    return Err(format!("Failed to top up EVM vault gas: {err}"));
                }
            }
        }

        evm_chain
            .refund_to_address(
                &token_identifier(&vault.deposit_asset.asset),
                private_key,
                &vault.recovery_address,
            )
            .await
            .map_err(|err| err.to_string())
    }

    async fn is_vault_funded(&self, vault: &DepositVault) -> VaultResult<bool> {
        let required_amount = self.required_funding_amount(vault).await?;
        let backend_chain = backend_chain_for_id(&vault.deposit_asset.chain).ok_or(
            VaultError::ChainNotSupported {
                chain: vault.deposit_asset.chain.clone(),
            },
        )?;
        let funded = match backend_chain {
            ChainType::Bitcoin => {
                let bitcoin_chain = self.chain_registry.get_bitcoin(&backend_chain).ok_or(
                    VaultError::ChainNotSupported {
                        chain: vault.deposit_asset.chain.clone(),
                    },
                )?;
                U256::from(
                    bitcoin_chain
                        .address_balance_sats(&vault.deposit_vault_address)
                        .await
                        .map_err(|source| VaultError::FundingCheck {
                            message: format!("failed to query bitcoin funding balance: {source}"),
                        })?,
                ) >= required_amount
            }
            ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base => {
                let evm_chain = self.chain_registry.get_evm(&backend_chain).ok_or(
                    VaultError::ChainNotSupported {
                        chain: vault.deposit_asset.chain.clone(),
                    },
                )?;
                let balance = match &vault.deposit_asset.asset {
                    AssetId::Native => evm_chain
                        .native_balance(&vault.deposit_vault_address)
                        .await
                        .map_err(|source| VaultError::FundingCheck {
                            message: format!(
                                "failed to query EVM native funding balance: {source}"
                            ),
                        })?,
                    AssetId::Reference(token) => evm_chain
                        .erc20_balance(token, &vault.deposit_vault_address)
                        .await
                        .map_err(|source| VaultError::FundingCheck {
                            message: format!("failed to query EVM token funding balance: {source}"),
                        })?,
                };
                balance >= required_amount
            }
            ChainType::Hyperliquid => {
                return Err(VaultError::ChainNotSupported {
                    chain: vault.deposit_asset.chain.clone(),
                });
            }
        };
        Ok(funded)
    }

    async fn required_funding_amount(&self, vault: &DepositVault) -> VaultResult<U256> {
        if let Some(order_id) = vault.order_id {
            let quote = self
                .db
                .orders()
                .get_market_order_quote(order_id)
                .await
                .map_err(VaultError::database)?;
            return parse_positive_u256("quote.amount_in", &quote.amount_in);
        }

        match &vault.action {
            VaultAction::Null => Ok(U256::from(1)),
            VaultAction::MarketOrder(action) => match &action.order_kind {
                crate::models::MarketOrderKind::ExactIn { amount_in, .. } => {
                    parse_positive_u256("amount_in", amount_in)
                }
                crate::models::MarketOrderKind::ExactOut { max_amount_in, .. } => {
                    parse_positive_u256("max_amount_in", max_amount_in)
                }
            },
        }
    }

    fn validate_and_normalize_deposit_asset(
        &self,
        deposit_asset: &DepositAsset,
    ) -> VaultResult<DepositAsset> {
        let backend_chain =
            backend_chain_for_id(&deposit_asset.chain).ok_or(VaultError::ChainNotSupported {
                chain: deposit_asset.chain.clone(),
            })?;
        match backend_chain {
            ChainType::Bitcoin => match &deposit_asset.asset {
                AssetId::Native => Ok(deposit_asset.clone()),
                AssetId::Reference(asset) => Err(VaultError::InvalidAssetId {
                    asset: asset.clone(),
                    chain: deposit_asset.chain.clone(),
                    reason: "bitcoin vaults only support the native asset".to_string(),
                }),
            },
            ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base => {
                match &deposit_asset.asset {
                    AssetId::Native => Ok(deposit_asset.clone()),
                    AssetId::Reference(asset) => {
                        deposit_asset.normalized_asset_identity().map_err(|reason| {
                            VaultError::InvalidAssetId {
                                asset: asset.clone(),
                                chain: deposit_asset.chain.clone(),
                                reason,
                            }
                        })
                    }
                }
            }
            ChainType::Hyperliquid => Err(VaultError::ChainNotSupported {
                chain: deposit_asset.chain.clone(),
            }),
        }
    }

    async fn validate_order_binding(
        &self,
        order_id: Uuid,
        deposit_asset: &DepositAsset,
        requested_action: &VaultAction,
    ) -> VaultResult<OrderBinding> {
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(VaultError::database)?;

        if order.status != RouterOrderStatus::Quoted {
            return Err(VaultError::InvalidOrderBinding {
                reason: format!(
                    "order {} is {}, expected quoted",
                    order.id,
                    order.status.to_db_string()
                ),
            });
        }
        if order.funding_vault_id.is_some() {
            return Err(VaultError::InvalidOrderBinding {
                reason: format!("order {} already has a funding vault", order.id),
            });
        }
        if order.source_asset != *deposit_asset {
            return Err(VaultError::InvalidOrderBinding {
                reason: "vault deposit asset must match order source asset".to_string(),
            });
        }

        let expected_action = VaultAction::from(order.action);
        if *requested_action != VaultAction::Null && *requested_action != expected_action {
            return Err(VaultError::InvalidOrderBinding {
                reason: "vault action must match the router order action".to_string(),
            });
        }

        let quote = self
            .db
            .orders()
            .get_market_order_quote(order_id)
            .await
            .map_err(VaultError::database)?;

        Ok(OrderBinding {
            action: expected_action,
            quote_id: quote.id,
        })
    }

    fn validate_and_normalize_recovery_address(
        &self,
        chain_id: &ChainId,
        address: &str,
    ) -> VaultResult<String> {
        let backend_chain =
            backend_chain_for_id(chain_id).ok_or(VaultError::ChainNotSupported {
                chain: chain_id.clone(),
            })?;
        let chain =
            self.chain_registry
                .get(&backend_chain)
                .ok_or(VaultError::ChainNotSupported {
                    chain: chain_id.clone(),
                })?;
        let normalized = if chain_id.evm_chain_id().is_some() {
            address.to_lowercase()
        } else {
            address.to_string()
        };
        if chain.validate_address(&normalized) {
            Ok(normalized)
        } else {
            Err(VaultError::InvalidRecoveryAddress {
                address: address.to_string(),
                chain: chain_id.clone(),
            })
        }
    }

    fn validate_metadata(metadata: &serde_json::Value) -> VaultResult<()> {
        if metadata.is_object() {
            return Ok(());
        }

        Err(VaultError::InvalidMetadata {
            reason: "metadata must be a JSON object".to_string(),
        })
    }

    fn verify_cancellation_secret(
        &self,
        cancellation_secret: &str,
        expected_commitment: &str,
    ) -> VaultResult<()> {
        let secret_bytes = decode_hex_32(cancellation_secret)
            .map_err(|_| VaultError::InvalidCancellationSecret)?;
        let expected_commitment = decode_hex_32(expected_commitment)
            .map_err(|_| VaultError::InvalidCancellationSecret)?;
        let actual_commitment = compute_cancellation_commitment(&secret_bytes);

        if actual_commitment == expected_commitment {
            Ok(())
        } else {
            Err(VaultError::InvalidCancellationSecret)
        }
    }

    fn ensure_refund_allowed(&self, vault: &DepositVault) -> VaultResult<()> {
        match vault.status {
            DepositVaultStatus::PendingFunding
            | DepositVaultStatus::Funded
            | DepositVaultStatus::Refunding => Ok(()),
            DepositVaultStatus::Executing => Err(VaultError::RefundNotAllowed {
                reason: "executing vaults cannot be cancelled".to_string(),
            }),
            DepositVaultStatus::RefundRequired => Err(VaultError::RefundNotAllowed {
                reason: "refund-required vaults cannot be cancelled".to_string(),
            }),
            DepositVaultStatus::Completed => Err(VaultError::RefundNotAllowed {
                reason: "completed vaults cannot be cancelled".to_string(),
            }),
            DepositVaultStatus::Refunded => Err(VaultError::RefundNotAllowed {
                reason: "refunded vaults cannot be cancelled".to_string(),
            }),
            DepositVaultStatus::RefundManualInterventionRequired => {
                Err(VaultError::RefundNotAllowed {
                    reason: "manual-refund vaults cannot be cancelled".to_string(),
                })
            }
        }
    }
}

fn token_identifier(asset_id: &AssetId) -> TokenIdentifier {
    match asset_id {
        AssetId::Native => TokenIdentifier::Native,
        AssetId::Reference(value) => TokenIdentifier::address(value.clone()),
    }
}

fn map_create_vault_error(source: RouterServerError) -> VaultError {
    match source {
        RouterServerError::Validation { message } => {
            VaultError::InvalidOrderBinding { reason: message }
        }
        source => VaultError::database(source),
    }
}

fn parse_positive_u256(field: &'static str, value: &str) -> VaultResult<U256> {
    let amount =
        U256::from_str_radix(value, 10).map_err(|err| VaultError::InvalidFundingAmount {
            field,
            reason: err.to_string(),
        })?;
    if amount == U256::ZERO {
        return Err(VaultError::InvalidFundingAmount {
            field,
            reason: "amount must be greater than zero".to_string(),
        });
    }
    Ok(amount)
}

fn compute_cancellation_commitment(secret_bytes: &[u8; 32]) -> [u8; 32] {
    keccak256([CANCELLATION_COMMITMENT_DOMAIN, secret_bytes].concat()).into()
}

fn normalize_hex_32(value: &str) -> Result<String, String> {
    let bytes = decode_hex_32(value)?;
    Ok(format!("0x{}", alloy::hex::encode(bytes)))
}

fn decode_hex_32(value: &str) -> Result<[u8; 32], String> {
    let stripped = value.strip_prefix("0x").unwrap_or(value);
    let bytes = alloy::hex::decode(stripped).map_err(|err| err.to_string())?;
    bytes
        .try_into()
        .map_err(|_| "expected 32-byte hex value".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_secret() -> [u8; 32] {
        [0xab; 32]
    }

    #[test]
    fn decode_hex_32_with_0x_prefix() {
        let hex = "0x".to_string() + &alloy::hex::encode([1u8; 32]);
        let result = decode_hex_32(&hex).unwrap();
        assert_eq!(result, [1u8; 32]);
    }

    #[test]
    fn decode_hex_32_without_prefix() {
        let hex = alloy::hex::encode([2u8; 32]);
        let result = decode_hex_32(&hex).unwrap();
        assert_eq!(result, [2u8; 32]);
    }

    #[test]
    fn decode_hex_32_rejects_wrong_length() {
        assert!(decode_hex_32("0xdeadbeef").is_err());
    }

    #[test]
    fn decode_hex_32_rejects_invalid_hex() {
        assert!(decode_hex_32("0xZZZZ").is_err());
    }

    #[test]
    fn normalize_hex_32_lowercases_and_prefixes() {
        let input = alloy::hex::encode([0xff; 32]).to_uppercase();
        let normalized = normalize_hex_32(&input).unwrap();
        assert!(normalized.starts_with("0x"));
        assert_eq!(normalized, normalized.to_lowercase());
    }

    #[test]
    fn normalize_hex_32_rejects_bad_length() {
        assert!(normalize_hex_32("0xdead").is_err());
    }

    #[test]
    fn cancellation_commitment_is_deterministic() {
        let secret = test_secret();
        let c1 = compute_cancellation_commitment(&secret);
        let c2 = compute_cancellation_commitment(&secret);
        assert_eq!(c1, c2);
    }

    #[test]
    fn cancellation_commitment_uses_domain_separation() {
        let secret = test_secret();
        let commitment = compute_cancellation_commitment(&secret);
        let raw_hash: [u8; 32] = keccak256(secret).into();
        assert_ne!(commitment, raw_hash);
    }

    #[test]
    fn cancellation_commitment_differs_for_different_secrets() {
        let c1 = compute_cancellation_commitment(&[0xaa; 32]);
        let c2 = compute_cancellation_commitment(&[0xbb; 32]);
        assert_ne!(c1, c2);
    }

    #[test]
    fn validate_metadata_accepts_object() {
        assert!(VaultManager::validate_metadata(&serde_json::json!({})).is_ok());
        assert!(VaultManager::validate_metadata(&serde_json::json!({"key": "value"})).is_ok());
    }

    #[test]
    fn validate_metadata_rejects_non_objects() {
        assert!(VaultManager::validate_metadata(&serde_json::json!([])).is_err());
        assert!(VaultManager::validate_metadata(&serde_json::json!("string")).is_err());
        assert!(VaultManager::validate_metadata(&serde_json::json!(42)).is_err());
        assert!(VaultManager::validate_metadata(&serde_json::json!(null)).is_err());
        assert!(VaultManager::validate_metadata(&serde_json::json!(true)).is_err());
    }
}
