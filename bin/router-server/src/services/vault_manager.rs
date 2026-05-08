use crate::{
    api::CreateVaultRequest,
    error::RouterServerError,
    services::deposit_address::{derive_deposit_address_for_quote, DepositAddressError},
    telemetry,
};
use alloy::primitives::{keccak256, U256};
use blockchain_utils::MempoolEsploraFeeExt;
use chains::ChainRegistry;
use chrono::{DateTime, Duration, Utc};
use hkdf::Hkdf;
use router_core::{
    config::Settings,
    db::Database,
    error::RouterCoreError,
    models::{
        DepositVault, DepositVaultFundingHint, DepositVaultFundingObservation, DepositVaultStatus,
        ProviderOperationHintStatus, RouterOrderQuote, RouterOrderStatus, VaultAction,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId, DepositAsset},
    services::bitcoin_funding::observed_bitcoin_outpoint,
};
use router_primitives::{ChainType, TokenIdentifier};
use serde_json::{json, Value};
use sha2::Sha256;
use snafu::Snafu;
use std::{collections::HashSet, fmt, sync::Arc, time::Instant};
use tracing::{info, warn};
use uuid::Uuid;

const CANCELLATION_COMMITMENT_DOMAIN: &[u8] = b"router-server-cancel-v1";
const ORDER_CANCELLATION_SECRET_DOMAIN: &[u8] = b"router-order-cancel-secret-v1";
const DEFAULT_CANCEL_AFTER: Duration = Duration::hours(24);
const REFUND_PASS_LIMIT: i64 = 100;
const REFUND_LEASE_DURATION: Duration = Duration::minutes(5);
const REFUND_RETRY_DELAY: Duration = Duration::seconds(30);
const BITCOIN_REFUND_FEE_VBYTES: f64 = 125.0;

#[derive(Clone, PartialEq, Eq)]
pub struct OrderCancellationMaterial {
    pub commitment: String,
    pub secret: String,
}

impl fmt::Debug for OrderCancellationMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderCancellationMaterial")
            .field("commitment", &self.commitment)
            .field("secret", &"<redacted>")
            .finish()
    }
}

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

    #[snafu(display("Funding hint is not observable yet: {}", reason))]
    FundingHintNotReady { reason: String },

    #[snafu(display("Invalid funding hint: {}", reason))]
    InvalidFundingHint { reason: String },

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

    fn database(source: impl Into<RouterServerError>) -> Self {
        Self::Database {
            source: Box::new(source.into()),
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
    Observed { vault: Box<DepositVault> },
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
            funding_observation: None,
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

    #[must_use]
    pub fn order_cancellation_material(&self, quote_id: Uuid) -> OrderCancellationMaterial {
        let secret_bytes =
            derive_order_cancellation_secret(&self.settings.master_key_bytes(), quote_id);
        OrderCancellationMaterial {
            commitment: compute_cancellation_commitment_hex(&secret_bytes),
            secret: format!("0x{}", alloy::hex::encode(secret_bytes)),
        }
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

    pub async fn record_funding_hint(
        &self,
        hint: DepositVaultFundingHint,
    ) -> VaultResult<DepositVaultFundingHint> {
        let vault = self
            .db
            .vaults()
            .get(hint.vault_id)
            .await
            .map_err(VaultError::database)?;
        self.ensure_full_amount_hint_is_observable(&vault, &hint)
            .await?;
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
                    if self
                        .complete_claimed_funding_hint(
                            &hint,
                            ProviderOperationHintStatus::Processed,
                            json!({}),
                        )
                        .await?
                    {
                        summary.processed += 1;
                    }
                    if let Some(order_id) = vault.order_id {
                        summary.funded_order_ids.push(order_id);
                    }
                }
                Ok(FundingHintDisposition::Observed { vault }) => {
                    if self
                        .complete_claimed_funding_hint(
                            &hint,
                            ProviderOperationHintStatus::Processed,
                            json!({}),
                        )
                        .await?
                    {
                        summary.processed += 1;
                    }
                    info!(
                        vault_id = %vault.id,
                        hint_id = %hint.id,
                        status = %vault.status.to_db_string(),
                        "Router worker recorded late funding observation"
                    );
                }
                Ok(FundingHintDisposition::Ignored { reason }) => {
                    if self
                        .complete_claimed_funding_hint(
                            &hint,
                            ProviderOperationHintStatus::Ignored,
                            json!({ "reason": reason }),
                        )
                        .await?
                    {
                        summary.processed += 1;
                    }
                }
                Err(error) => {
                    if self
                        .complete_claimed_funding_hint(
                            &hint,
                            ProviderOperationHintStatus::Failed,
                            json!({ "error": error.to_string() }),
                        )
                        .await?
                    {
                        summary.processed += 1;
                    }
                }
            }
        }

        Ok(summary)
    }

    pub async fn reconcile_pending_funding_balances(&self, limit: i64) -> VaultResult<Vec<Uuid>> {
        let vaults = self
            .db
            .vaults()
            .find_pending_funding_without_observation(limit)
            .await
            .map_err(VaultError::database)?;
        let mut funded_order_ids = Vec::new();

        for vault in vaults {
            if !self.vault_supports_balance_reconciliation(&vault) {
                continue;
            }

            let required_amount = self.required_funding_amount(&vault).await?;
            let visible_balance = self.vault_visible_balance(&vault).await?;
            if visible_balance < required_amount {
                continue;
            }

            let now = Utc::now();
            let observation =
                funding_observation_from_balance_reconciliation(&vault, visible_balance, now);
            let funded_vault = match self
                .db
                .vaults()
                .mark_funded_with_observation(vault.id, &observation, now)
                .await
            {
                Ok(funded_vault) => funded_vault,
                Err(RouterCoreError::NotFound) => continue,
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
                visible_balance = %visible_balance,
                required_amount = %required_amount,
                "Router worker reconciled vault funding from chain balance"
            );
            if let Some(order_id) = funded_vault.order_id {
                funded_order_ids.push(order_id);
            }
        }

        Ok(funded_order_ids)
    }

    fn vault_supports_balance_reconciliation(&self, vault: &DepositVault) -> bool {
        matches!(
            backend_chain_for_id(&vault.deposit_asset.chain),
            Some(ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base)
        )
    }

    async fn complete_claimed_funding_hint(
        &self,
        hint: &DepositVaultFundingHint,
        status: ProviderOperationHintStatus,
        error: Value,
    ) -> VaultResult<bool> {
        match self
            .db
            .vaults()
            .complete_funding_hint(hint.id, hint.claimed_at, status, error, Utc::now())
            .await
        {
            Ok(_) => Ok(true),
            Err(RouterCoreError::NotFound) => {
                warn!(
                    hint_id = %hint.id,
                    claimed_at = ?hint.claimed_at,
                    "Funding hint completion lost its processing lease; leaving current hint owner to finish it"
                );
                Ok(false)
            }
            Err(source) => Err(VaultError::database(source)),
        }
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
        let should_refund_observed_deposit = match vault.status {
            DepositVaultStatus::PendingFunding => false,
            DepositVaultStatus::Funded => {
                return Ok(FundingHintDisposition::Ignored {
                    reason: "vault already funded".to_string(),
                });
            }
            DepositVaultStatus::Refunding if vault.funding_observation.is_none() => true,
            DepositVaultStatus::Refunding => {
                return Ok(FundingHintDisposition::Ignored {
                    reason: "refunding vault already has a funding observation".to_string(),
                });
            }
            status => {
                return Ok(FundingHintDisposition::Ignored {
                    reason: format!("vault status is {}", status.to_db_string()),
                });
            }
        };

        let required_amount = self.required_funding_amount(&vault).await?;
        if !self
            .is_vault_funded_by_hint(&vault, hint, required_amount)
            .await?
        {
            return Ok(FundingHintDisposition::Ignored {
                reason: "vault balance is still below required funding amount".to_string(),
            });
        }

        let observation = funding_observation_from_hint(&vault, hint)?;
        if should_refund_observed_deposit {
            let observed_vault = match self
                .db
                .vaults()
                .record_refunding_observation(vault.id, &observation, Utc::now())
                .await
            {
                Ok(observed_vault) => observed_vault,
                Err(RouterCoreError::NotFound) => {
                    return Ok(FundingHintDisposition::Ignored {
                        reason: "refunding vault observation was already recorded".to_string(),
                    });
                }
                Err(source) => return Err(VaultError::database(source)),
            };
            return Ok(FundingHintDisposition::Observed {
                vault: Box::new(observed_vault),
            });
        }

        let funded_vault = match self
            .db
            .vaults()
            .mark_funded_with_observation(vault.id, &observation, Utc::now())
            .await
        {
            Ok(funded_vault) => funded_vault,
            Err(RouterCoreError::NotFound) => {
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
        let timeout_claimed_until = now + REFUND_LEASE_DURATION;
        match self
            .db
            .vaults()
            .claim_due_for_timeout(
                now,
                timeout_claimed_until,
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
                    match self
                        .attempt_refund(vault.clone(), timeout_claimed_until)
                        .await
                    {
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
        let retry_claimed_until = retry_pass_started_at + REFUND_LEASE_DURATION;
        let retry_pass_started = Instant::now();
        match self
            .db
            .vaults()
            .claim_refunding(
                retry_pass_started_at,
                retry_claimed_until,
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
                    match self.attempt_refund(vault, retry_claimed_until).await {
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

    async fn attempt_refund(
        &self,
        vault: DepositVault,
        claimed_until: DateTime<Utc>,
    ) -> VaultResult<DepositVault> {
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
                let refunded_vault = match self
                    .db
                    .vaults()
                    .mark_refunded(
                        vault.id,
                        Utc::now(),
                        &tx_hash,
                        &self.worker_id,
                        claimed_until,
                    )
                    .await
                {
                    Ok(vault) => vault,
                    Err(RouterCoreError::NotFound) => {
                        warn!(
                            vault_id = %vault.id,
                            worker_id = %self.worker_id,
                            claimed_until = %claimed_until,
                            "Refund success lost its processing lease; leaving current refund owner to finish it"
                        );
                        return Ok(vault);
                    }
                    Err(source) => return Err(VaultError::database(source)),
                };
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
                if refund_error_requires_manual_intervention(&message) {
                    match self
                        .db
                        .vaults()
                        .mark_refund_manual_intervention_required(
                            vault.id,
                            failed_at,
                            &message,
                            &self.worker_id,
                            claimed_until,
                        )
                        .await
                    {
                        Ok(vault) => {
                            telemetry::record_vault_transition(
                                &vault,
                                DepositVaultStatus::Refunding,
                                vault.status,
                            );
                            return Ok(vault);
                        }
                        Err(RouterCoreError::NotFound) => {
                            warn!(
                                vault_id = %vault.id,
                                worker_id = %self.worker_id,
                                claimed_until = %claimed_until,
                                "Refund manual-intervention transition lost its processing lease; leaving current refund owner to finish it"
                            );
                            return Ok(vault);
                        }
                        Err(source) => return Err(VaultError::database(source)),
                    }
                }
                match self
                    .db
                    .vaults()
                    .mark_refund_error(
                        vault.id,
                        failed_at,
                        failed_at + REFUND_RETRY_DELAY,
                        &message,
                        &self.worker_id,
                        claimed_until,
                    )
                    .await
                {
                    Ok(vault) => Ok(vault),
                    Err(RouterCoreError::NotFound) => {
                        warn!(
                            vault_id = %vault.id,
                            worker_id = %self.worker_id,
                            claimed_until = %claimed_until,
                            "Refund failure lost its processing lease; leaving current refund owner to finish it"
                        );
                        Ok(vault)
                    }
                    Err(source) => Err(VaultError::database(source)),
                }
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
        if let Some(outpoint) = observed_bitcoin_outpoint(vault.funding_observation.as_ref())? {
            let fee = bitcoin_chain
                .estimate_p2wpkh_transfer_fee_sats(1, 1)
                .await
                .map_err(|err| err.to_string())?;
            let tx_data = bitcoin_chain
                .dump_to_address_from_outpoint(
                    &token_identifier(&vault.deposit_asset.asset),
                    private_key,
                    &vault.recovery_address,
                    U256::from(fee),
                    &outpoint.tx_hash,
                    outpoint.vout,
                    outpoint.amount_sats,
                )
                .await
                .map_err(|err| err.to_string())?;
            return bitcoin_chain
                .broadcast_signed_transaction(&tx_data)
                .await
                .map_err(|err| err.to_string());
        }
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
        let fee = bitcoin_refund_fee_sats(next_block_fee_rate)?;

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

    async fn vault_visible_balance(&self, vault: &DepositVault) -> VaultResult<U256> {
        let backend_chain = backend_chain_for_id(&vault.deposit_asset.chain).ok_or(
            VaultError::ChainNotSupported {
                chain: vault.deposit_asset.chain.clone(),
            },
        )?;
        match backend_chain {
            ChainType::Bitcoin => {
                let bitcoin_chain = self.chain_registry.get_bitcoin(&backend_chain).ok_or(
                    VaultError::ChainNotSupported {
                        chain: vault.deposit_asset.chain.clone(),
                    },
                )?;
                Ok(U256::from(
                    bitcoin_chain
                        .address_balance_sats(&vault.deposit_vault_address)
                        .await
                        .map_err(|source| VaultError::FundingCheck {
                            message: format!("failed to query bitcoin funding balance: {source}"),
                        })?,
                ))
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
                Ok(balance)
            }
            ChainType::Hyperliquid => Err(VaultError::ChainNotSupported {
                chain: vault.deposit_asset.chain.clone(),
            }),
        }
    }

    async fn ensure_full_amount_hint_is_observable(
        &self,
        vault: &DepositVault,
        hint: &DepositVaultFundingHint,
    ) -> VaultResult<()> {
        if vault.status != DepositVaultStatus::PendingFunding {
            return Ok(());
        }
        let required_amount = self.required_funding_amount(vault).await?;
        if funding_hint_observed_amount(hint)?
            .is_none_or(|observed_amount| observed_amount < required_amount)
        {
            return Ok(());
        }
        if self
            .is_vault_funded_by_hint(vault, hint, required_amount)
            .await?
        {
            return Ok(());
        }
        Err(VaultError::FundingHintNotReady {
            reason: "full-amount funding hint is not visible to router chain backend yet"
                .to_string(),
        })
    }

    async fn is_vault_funded_by_hint(
        &self,
        vault: &DepositVault,
        hint: &DepositVaultFundingHint,
        required_amount: U256,
    ) -> VaultResult<bool> {
        let backend_chain = backend_chain_for_id(&vault.deposit_asset.chain).ok_or(
            VaultError::ChainNotSupported {
                chain: vault.deposit_asset.chain.clone(),
            },
        )?;
        if backend_chain == ChainType::Bitcoin
            && funding_hint_confirmation_state(hint).as_deref() == Some("mempool")
        {
            return self
                .is_bitcoin_mempool_hint_spendable(vault, hint, required_amount)
                .await;
        }
        let visible_balance = self.vault_visible_balance(vault).await?;
        if let Some(observed_amount) = funding_hint_observed_amount(hint)? {
            if visible_balance < observed_amount {
                return Ok(false);
            }
        }
        Ok(visible_balance >= required_amount)
    }

    async fn is_bitcoin_mempool_hint_spendable(
        &self,
        vault: &DepositVault,
        hint: &DepositVaultFundingHint,
        required_amount: U256,
    ) -> VaultResult<bool> {
        let observed_amount = match funding_hint_observed_amount(hint)? {
            Some(amount) if amount >= required_amount => amount,
            _ => return Ok(false),
        };
        if observed_amount > U256::from(u64::MAX) {
            return Err(VaultError::FundingCheck {
                message: "bitcoin funding hint amount exceeds satoshi range".to_string(),
            });
        }
        let tx_hash = funding_hint_tx_hash(hint).ok_or_else(|| VaultError::FundingCheck {
            message: "bitcoin mempool funding hint missing tx_hash".to_string(),
        })?;
        let vout = funding_hint_vout(hint)?.ok_or_else(|| VaultError::FundingCheck {
            message: "bitcoin mempool funding hint missing vout".to_string(),
        })?;

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
        let bitcoin_chain = self.chain_registry.get_bitcoin(&backend_chain).ok_or(
            VaultError::ChainNotSupported {
                chain: vault.deposit_asset.chain.clone(),
            },
        )?;

        bitcoin_chain
            .can_spend_outpoint_now(
                deposit_wallet.private_key(),
                &vault.recovery_address,
                &tx_hash,
                vout,
                observed_amount.to::<u64>(),
            )
            .await
            .map_err(|source| VaultError::FundingCheck {
                message: format!("failed to test bitcoin mempool spendability: {source}"),
            })
    }

    async fn required_funding_amount(&self, vault: &DepositVault) -> VaultResult<U256> {
        if let Some(order_id) = vault.order_id {
            let quote = self
                .db
                .orders()
                .get_router_order_quote(order_id)
                .await
                .map_err(VaultError::database)?;
            return match quote {
                RouterOrderQuote::MarketOrder(quote) => {
                    parse_positive_u256("quote.amount_in", &quote.amount_in)
                }
                RouterOrderQuote::LimitOrder(quote) => {
                    parse_positive_u256("quote.input_amount", &quote.input_amount)
                }
            };
        }

        match &vault.action {
            VaultAction::Null => Ok(U256::from(1)),
            VaultAction::MarketOrder(action) => match &action.order_kind {
                router_core::models::MarketOrderKind::ExactIn { amount_in, .. } => {
                    parse_positive_u256("amount_in", amount_in)
                }
                router_core::models::MarketOrderKind::ExactOut { max_amount_in, .. } => {
                    let Some(max_amount_in) = max_amount_in.as_deref() else {
                        return Err(VaultError::InvalidOrderBinding {
                            reason: "exact-out market order funding requires max_amount_in"
                                .to_string(),
                        });
                    };
                    parse_positive_u256("max_amount_in", max_amount_in)
                }
            },
            VaultAction::LimitOrder(action) => {
                parse_positive_u256("limit_order.input_amount", &action.input_amount)
            }
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
            .get_router_order_quote(order_id)
            .await
            .map_err(VaultError::database)?;

        Ok(OrderBinding {
            action: expected_action,
            quote_id: quote.id(),
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
            DepositVaultStatus::ManualInterventionRequired => Err(VaultError::RefundNotAllowed {
                reason: "manual-intervention vaults cannot be cancelled".to_string(),
            }),
            DepositVaultStatus::RefundManualInterventionRequired => {
                Err(VaultError::RefundNotAllowed {
                    reason: "manual-refund vaults cannot be cancelled".to_string(),
                })
            }
        }
    }
}

fn refund_error_requires_manual_intervention(message: &str) -> bool {
    message.contains("Observed bitcoin outpoint")
        && (message.contains(" is not spendable")
            || message.contains(" did not match expected ")
            || message.contains(" does not pay the signer address"))
}

fn funding_observation_from_hint(
    vault: &DepositVault,
    hint: &DepositVaultFundingHint,
) -> VaultResult<DepositVaultFundingObservation> {
    let evidence = hint.evidence.clone();
    let sender_addresses = funding_sender_addresses(&evidence)?;
    let sender_address = optional_string_field(&evidence, "sender_address")?
        .or_else(|| sender_addresses.first().cloned());
    let observed_amount = optional_decimal_u256_field(&evidence, "amount")?;
    Ok(DepositVaultFundingObservation {
        tx_hash: optional_string_field(&evidence, "tx_hash")?,
        sender_address,
        sender_addresses,
        recipient_address: optional_string_field(&evidence, "recipient_address")?
            .or(optional_string_field(&evidence, "address")?)
            .or_else(|| Some(vault.deposit_vault_address.clone())),
        transfer_index: optional_u64_field(&evidence, "transfer_index")?
            .or(optional_u64_field(&evidence, "vout")?),
        observed_amount: observed_amount.map(|amount| amount.to_string()),
        confirmation_state: optional_string_field(&evidence, "confirmation_state")?,
        observed_at: observed_at_from_hint(&evidence)?,
        evidence,
    })
}

fn funding_observation_from_balance_reconciliation(
    vault: &DepositVault,
    visible_balance: U256,
    observed_at: DateTime<Utc>,
) -> DepositVaultFundingObservation {
    DepositVaultFundingObservation {
        tx_hash: None,
        sender_address: None,
        sender_addresses: Vec::new(),
        recipient_address: Some(vault.deposit_vault_address.clone()),
        transfer_index: None,
        observed_amount: Some(visible_balance.to_string()),
        confirmation_state: Some("confirmed".to_string()),
        observed_at: Some(observed_at),
        evidence: json!({
            "source": "router_balance_reconciliation",
            "chain_id": vault.deposit_asset.chain.as_str(),
            "asset_id": vault.deposit_asset.asset.as_str(),
            "address": &vault.deposit_vault_address,
            "amount": visible_balance.to_string(),
            "confirmation_state": "confirmed",
            "observed_at": observed_at.to_rfc3339(),
        }),
    }
}

fn funding_sender_addresses(evidence: &Value) -> VaultResult<Vec<String>> {
    let mut addresses = Vec::new();
    if let Some(value) = evidence.get("sender_addresses") {
        let values = value
            .as_array()
            .ok_or_else(|| VaultError::InvalidFundingHint {
                reason: "sender_addresses must be an array".to_string(),
            })?;
        for (index, value) in values.iter().enumerate() {
            let address = value
                .as_str()
                .ok_or_else(|| VaultError::InvalidFundingHint {
                    reason: format!("sender_addresses[{index}] must be a string"),
                })?
                .trim();
            if address.is_empty() {
                return Err(VaultError::InvalidFundingHint {
                    reason: format!("sender_addresses[{index}] must be non-empty"),
                });
            }
            addresses.push(address.to_owned());
        }
    }
    if addresses.is_empty() {
        if let Some(sender) = optional_string_field(evidence, "sender_address")? {
            addresses.push(sender);
        }
    }
    let mut seen = HashSet::new();
    Ok(addresses
        .into_iter()
        .filter(|address| seen.insert(address.clone()))
        .collect())
}

fn optional_string_field(value: &Value, key: &'static str) -> VaultResult<Option<String>> {
    let Some(value) = value.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let string = value
        .as_str()
        .ok_or_else(|| VaultError::InvalidFundingHint {
            reason: format!("{key} must be a string"),
        })?;
    let string = string.trim();
    if string.is_empty() {
        return Ok(None);
    }
    Ok(Some(string.to_owned()))
}

fn optional_u64_field(value: &Value, key: &'static str) -> VaultResult<Option<u64>> {
    let Some(value) = value.get(key) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| VaultError::InvalidFundingHint {
            reason: format!("{key} must be an unsigned integer"),
        })
}

fn observed_at_from_hint(evidence: &Value) -> VaultResult<Option<DateTime<Utc>>> {
    let Some(value) = optional_string_field(evidence, "observed_at")? else {
        return Ok(None);
    };
    DateTime::parse_from_rfc3339(&value)
        .map(|value| Some(value.with_timezone(&Utc)))
        .map_err(|err| VaultError::InvalidFundingHint {
            reason: format!("observed_at must be RFC3339: {err}"),
        })
}

fn token_identifier(asset_id: &AssetId) -> TokenIdentifier {
    match asset_id {
        AssetId::Native => TokenIdentifier::Native,
        AssetId::Reference(value) => TokenIdentifier::address(value.clone()),
    }
}

fn map_create_vault_error(source: RouterCoreError) -> VaultError {
    match source {
        RouterCoreError::Validation { message } => {
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

fn funding_hint_observed_amount(hint: &DepositVaultFundingHint) -> VaultResult<Option<U256>> {
    optional_decimal_u256_field(&hint.evidence, "amount")
}

fn optional_decimal_u256_field(value: &Value, key: &'static str) -> VaultResult<Option<U256>> {
    let Some(raw) = value.get(key) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let Some(raw) = raw.as_str() else {
        return Err(VaultError::InvalidFundingHint {
            reason: format!("{key} must be a decimal string"),
        });
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    U256::from_str_radix(raw, 10)
        .map(Some)
        .map_err(|err| VaultError::InvalidFundingHint {
            reason: format!("{key} must be a valid decimal U256: {err}"),
        })
}

fn funding_hint_confirmation_state(hint: &DepositVaultFundingHint) -> Option<String> {
    hint.evidence
        .get("confirmation_state")
        .and_then(|value| value.as_str())
        .map(str::to_ascii_lowercase)
}

fn funding_hint_tx_hash(hint: &DepositVaultFundingHint) -> Option<String> {
    hint.evidence
        .get("tx_hash")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn funding_hint_vout(hint: &DepositVaultFundingHint) -> VaultResult<Option<u32>> {
    let Some((key, value)) = hint
        .evidence
        .get("vout")
        .map(|value| ("vout", value))
        .or_else(|| {
            hint.evidence
                .get("transfer_index")
                .map(|value| ("transfer_index", value))
        })
    else {
        return Ok(None);
    };
    let value = value
        .as_u64()
        .ok_or_else(|| VaultError::InvalidFundingHint {
            reason: format!("{key} must be a non-negative integer"),
        })?;
    u32::try_from(value)
        .map(Some)
        .map_err(|_err| VaultError::InvalidFundingHint {
            reason: format!("{key} exceeds bitcoin vout u32 range"),
        })
}

fn bitcoin_refund_fee_sats(next_block_fee_rate_sat_per_vb: f64) -> Result<u64, String> {
    if !next_block_fee_rate_sat_per_vb.is_finite() || next_block_fee_rate_sat_per_vb <= 0.0 {
        return Err(format!(
            "bitcoin refund fee rate must be finite and positive: {next_block_fee_rate_sat_per_vb}"
        ));
    }

    let fee = (next_block_fee_rate_sat_per_vb * BITCOIN_REFUND_FEE_VBYTES).ceil();
    if !fee.is_finite() || fee >= u64::MAX as f64 {
        return Err(format!(
            "bitcoin refund fee exceeds u64 satoshi range: {fee}"
        ));
    }
    Ok(fee as u64)
}

pub(crate) fn compute_cancellation_commitment(secret_bytes: &[u8; 32]) -> [u8; 32] {
    keccak256([CANCELLATION_COMMITMENT_DOMAIN, secret_bytes].concat()).into()
}

pub(crate) fn compute_cancellation_commitment_hex(secret_bytes: &[u8; 32]) -> String {
    format!(
        "0x{}",
        alloy::hex::encode(compute_cancellation_commitment(secret_bytes))
    )
}

fn derive_order_cancellation_secret(master_key: &[u8; 64], quote_id: Uuid) -> [u8; 32] {
    let hkdf = Hkdf::<Sha256>::new(Some(ORDER_CANCELLATION_SECRET_DOMAIN), master_key);
    let mut secret = [0_u8; 32];
    hkdf.expand(quote_id.as_bytes(), &mut secret)
        .expect("router order cancellation secret length is valid for HKDF-SHA256");
    secret
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
    fn order_cancellation_material_debug_redacts_secret() {
        let material = OrderCancellationMaterial {
            commitment: "0xcommitment".to_string(),
            secret: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
        };

        let rendered = format!("{material:?}");
        assert!(rendered.contains("0xcommitment"));
        assert!(rendered.contains("secret"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("aaaaaaaaaaaaaaaa"));
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
    fn order_cancellation_secret_is_recoverable_by_quote_and_master_key() {
        let master_key = [0x42_u8; 64];
        let quote_id = Uuid::parse_str("019557a1-0000-7000-8000-000000000001").unwrap();
        let secret = derive_order_cancellation_secret(&master_key, quote_id);

        assert_eq!(
            secret,
            derive_order_cancellation_secret(&master_key, quote_id)
        );
        assert_ne!(
            secret,
            derive_order_cancellation_secret(
                &master_key,
                Uuid::parse_str("019557a1-0000-7000-8000-000000000002").unwrap()
            )
        );
        assert_ne!(
            secret,
            derive_order_cancellation_secret(&[0x43_u8; 64], quote_id)
        );
    }

    #[test]
    fn order_cancellation_material_matches_commitment() {
        let master_key = [0x42_u8; 64];
        let quote_id = Uuid::parse_str("019557a1-0000-7000-8000-000000000001").unwrap();
        let secret = derive_order_cancellation_secret(&master_key, quote_id);
        let material = OrderCancellationMaterial {
            commitment: compute_cancellation_commitment_hex(&secret),
            secret: format!("0x{}", alloy::hex::encode(secret)),
        };

        assert_eq!(material.secret.len(), 66);
        assert_eq!(
            material.commitment,
            compute_cancellation_commitment_hex(&secret)
        );
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

    #[test]
    fn funding_hint_vout_accepts_vout_or_transfer_index() {
        let mut hint = DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: Uuid::now_v7(),
            source: "sauron".to_string(),
            hint_kind: router_core::models::ProviderOperationHintKind::PossibleProgress,
            evidence: serde_json::json!({ "vout": 7 }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: None,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert_eq!(funding_hint_vout(&hint).unwrap(), Some(7));

        hint.evidence = serde_json::json!({ "transfer_index": 8 });
        assert_eq!(funding_hint_vout(&hint).unwrap(), Some(8));
    }

    #[test]
    fn funding_hint_vout_rejects_malformed_or_overflowing_values() {
        let mut hint = DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: Uuid::now_v7(),
            source: "sauron".to_string(),
            hint_kind: router_core::models::ProviderOperationHintKind::PossibleProgress,
            evidence: serde_json::json!({ "vout": "7" }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: None,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert!(matches!(
            funding_hint_vout(&hint),
            Err(VaultError::InvalidFundingHint { .. })
        ));

        hint.evidence = serde_json::json!({ "transfer_index": u64::from(u32::MAX) + 1 });
        assert!(matches!(
            funding_hint_vout(&hint),
            Err(VaultError::InvalidFundingHint { .. })
        ));
    }

    #[test]
    fn funding_hint_confirmation_state_is_normalized() {
        let hint = DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: Uuid::now_v7(),
            source: "sauron".to_string(),
            hint_kind: router_core::models::ProviderOperationHintKind::PossibleProgress,
            evidence: serde_json::json!({ "confirmation_state": "Mempool" }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: None,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert_eq!(
            funding_hint_confirmation_state(&hint).as_deref(),
            Some("mempool")
        );
    }

    #[test]
    fn bitcoin_refund_fee_sats_rejects_invalid_fee_rates() {
        assert_eq!(bitcoin_refund_fee_sats(1.01).unwrap(), 127);
        assert_eq!(bitcoin_refund_fee_sats(0.001).unwrap(), 1);

        assert!(bitcoin_refund_fee_sats(0.0).is_err());
        assert!(bitcoin_refund_fee_sats(-1.0).is_err());
        assert!(bitcoin_refund_fee_sats(f64::NAN).is_err());
        assert!(bitcoin_refund_fee_sats(f64::INFINITY).is_err());
        assert!(
            bitcoin_refund_fee_sats((u64::MAX as f64 / BITCOIN_REFUND_FEE_VBYTES) + 1.0).is_err()
        );
    }

    #[test]
    fn funding_observation_from_hint_preserves_all_sender_addresses() {
        let now = Utc::now();
        let vault = DepositVault {
            id: Uuid::now_v7(),
            order_id: Some(Uuid::now_v7()),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::parse("native").unwrap(),
            },
            action: VaultAction::Null,
            metadata: serde_json::json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "bcrt1qrecipient".to_string(),
            recovery_address: "bcrt1qrecovery".to_string(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: now + Duration::minutes(10),
            status: DepositVaultStatus::PendingFunding,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            funding_observation: None,
            created_at: now,
            updated_at: now,
        };
        let hint = DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: vault.id,
            source: "sauron".to_string(),
            hint_kind: router_core::models::ProviderOperationHintKind::PossibleProgress,
            evidence: serde_json::json!({
                "tx_hash": "abc123",
                "sender_address": "sender-a",
                "sender_addresses": ["sender-a", "sender-b"],
                "recipient_address": "bcrt1qrecipient",
                "vout": 2,
                "amount": "42",
                "confirmation_state": "mempool",
                "observed_at": now.to_rfc3339(),
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: None,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        };

        let observation = funding_observation_from_hint(&vault, &hint).unwrap();

        assert_eq!(observation.tx_hash.as_deref(), Some("abc123"));
        assert_eq!(observation.sender_address.as_deref(), Some("sender-a"));
        assert_eq!(observation.sender_addresses, vec!["sender-a", "sender-b"]);
        assert_eq!(
            observation.recipient_address.as_deref(),
            Some("bcrt1qrecipient")
        );
        assert_eq!(observation.transfer_index, Some(2));
        assert_eq!(observation.observed_amount.as_deref(), Some("42"));
        assert_eq!(observation.confirmation_state.as_deref(), Some("mempool"));
    }

    #[test]
    fn funding_observation_from_balance_reconciliation_records_chain_truth_without_tx_hash() {
        let now = Utc::now();
        let vault = DepositVault {
            id: Uuid::now_v7(),
            order_id: Some(Uuid::now_v7()),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::parse("native").unwrap(),
            },
            action: VaultAction::Null,
            metadata: serde_json::json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "0x1111111111111111111111111111111111111111".to_string(),
            recovery_address: "0x2222222222222222222222222222222222222222".to_string(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: now + Duration::minutes(10),
            status: DepositVaultStatus::PendingFunding,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            funding_observation: None,
            created_at: now,
            updated_at: now,
        };

        let observation =
            funding_observation_from_balance_reconciliation(&vault, U256::from(42_u64), now);

        assert!(observation.tx_hash.is_none());
        assert_eq!(observation.sender_addresses, Vec::<String>::new());
        assert_eq!(
            observation.recipient_address.as_deref(),
            Some("0x1111111111111111111111111111111111111111")
        );
        assert_eq!(observation.observed_amount.as_deref(), Some("42"));
        assert_eq!(observation.confirmation_state.as_deref(), Some("confirmed"));
        assert_eq!(
            observation.evidence["source"],
            serde_json::json!("router_balance_reconciliation")
        );
    }

    #[test]
    fn funding_observation_from_hint_rejects_malformed_sender_addresses() {
        let now = Utc::now();
        let vault = DepositVault {
            id: Uuid::now_v7(),
            order_id: Some(Uuid::now_v7()),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::parse("native").unwrap(),
            },
            action: VaultAction::Null,
            metadata: serde_json::json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "bcrt1qrecipient".to_string(),
            recovery_address: "bcrt1qrecovery".to_string(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: now + Duration::minutes(10),
            status: DepositVaultStatus::PendingFunding,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            funding_observation: None,
            created_at: now,
            updated_at: now,
        };
        let mut hint = DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: vault.id,
            source: "sauron".to_string(),
            hint_kind: router_core::models::ProviderOperationHintKind::PossibleProgress,
            evidence: serde_json::json!({
                "sender_addresses": ["sender-a", 7],
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: None,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        };

        assert!(matches!(
            funding_observation_from_hint(&vault, &hint),
            Err(VaultError::InvalidFundingHint { reason })
                if reason.contains("sender_addresses[1] must be a string")
        ));

        hint.evidence = serde_json::json!({ "tx_hash": 7 });
        assert!(matches!(
            funding_observation_from_hint(&vault, &hint),
            Err(VaultError::InvalidFundingHint { reason })
                if reason.contains("tx_hash must be a string")
        ));

        hint.evidence = serde_json::json!({ "vout": "2" });
        assert!(matches!(
            funding_observation_from_hint(&vault, &hint),
            Err(VaultError::InvalidFundingHint { reason })
                if reason.contains("vout must be an unsigned integer")
        ));

        hint.evidence = serde_json::json!({ "observed_at": 7 });
        assert!(matches!(
            funding_observation_from_hint(&vault, &hint),
            Err(VaultError::InvalidFundingHint { reason })
                if reason.contains("observed_at must be a string")
        ));
    }

    #[test]
    fn funding_observation_from_hint_rejects_invalid_observed_at() {
        let now = Utc::now();
        let vault = DepositVault {
            id: Uuid::now_v7(),
            order_id: Some(Uuid::now_v7()),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::parse("native").unwrap(),
            },
            action: VaultAction::Null,
            metadata: serde_json::json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "bcrt1qrecipient".to_string(),
            recovery_address: "bcrt1qrecovery".to_string(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: now + Duration::minutes(10),
            status: DepositVaultStatus::PendingFunding,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            funding_observation: None,
            created_at: now,
            updated_at: now,
        };
        let hint = DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: vault.id,
            source: "sauron".to_string(),
            hint_kind: router_core::models::ProviderOperationHintKind::PossibleProgress,
            evidence: serde_json::json!({
                "observed_at": "definitely not a timestamp",
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: None,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        };

        assert!(funding_observation_from_hint(&vault, &hint).is_err());
    }

    #[test]
    fn funding_observation_from_hint_rejects_invalid_amount() {
        let now = Utc::now();
        let vault = DepositVault {
            id: Uuid::now_v7(),
            order_id: Some(Uuid::now_v7()),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::parse("native").unwrap(),
            },
            action: VaultAction::Null,
            metadata: serde_json::json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "bcrt1qrecipient".to_string(),
            recovery_address: "bcrt1qrecovery".to_string(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: now + Duration::minutes(10),
            status: DepositVaultStatus::PendingFunding,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            funding_observation: None,
            created_at: now,
            updated_at: now,
        };
        let hint = DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: vault.id,
            source: "sauron".to_string(),
            hint_kind: router_core::models::ProviderOperationHintKind::PossibleProgress,
            evidence: serde_json::json!({
                "amount": "not-a-number",
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: None,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        };

        assert!(funding_hint_observed_amount(&hint).is_err());
        assert!(funding_observation_from_hint(&vault, &hint).is_err());
    }
}
