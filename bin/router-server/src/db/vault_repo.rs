use crate::{
    error::{RouterServerError, RouterServerResult},
    models::{
        CustodyVaultControlType, CustodyVaultRole, CustodyVaultStatus, CustodyVaultVisibility,
        DepositVault, DepositVaultFundingHint, DepositVaultFundingObservation, DepositVaultStatus,
        OrderExecutionStepStatus, OrderExecutionStepType, ProviderOperationHintKind,
        ProviderOperationHintStatus, VaultAction, SAURON_DETECTOR_HINT_SOURCE,
    },
    protocol::{AssetId, ChainId, DepositAsset},
    telemetry,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use std::time::Instant;
use uuid::Uuid;

const SELECT_COLUMNS: &str = r#"
    dv.id,
    cv.order_id,
    cv.chain_id AS deposit_chain_id,
    cv.asset_id AS deposit_asset_id,
    dv.action,
    dv.metadata,
    cv.derivation_salt AS deposit_vault_salt,
    cv.address AS deposit_vault_address,
    dv.recovery_address,
    dv.cancellation_commitment,
    dv.cancel_after,
    dv.status,
    dv.refund_requested_at,
    dv.refunded_at,
    dv.refund_tx_hash,
    dv.last_refund_error,
    dv.funding_tx_hash,
    dv.funding_sender_address,
    dv.funding_sender_addresses,
    dv.funding_recipient_address,
    dv.funding_transfer_index,
    dv.funding_observed_amount,
    dv.funding_confirmation_state,
    dv.funding_observed_at,
    dv.funding_evidence_json,
    dv.created_at,
    dv.updated_at
"#;

const SELECT_FROM: &str = "deposit_vaults dv JOIN custody_vaults cv ON cv.id = dv.id";

const FUNDING_HINT_SELECT_COLUMNS: &str = r#"
    id,
    vault_id,
    source,
    hint_kind,
    evidence_json,
    status,
    idempotency_key,
    error_json,
    claimed_at,
    processed_at,
    created_at,
    updated_at
"#;

const FUNDING_HINT_RETURNING_COLUMNS: &str = r#"
    hints.id,
    hints.vault_id,
    hints.source,
    hints.hint_kind,
    hints.evidence_json,
    hints.status,
    hints.idempotency_key,
    hints.error_json,
    hints.claimed_at,
    hints.processed_at,
    hints.created_at,
    hints.updated_at
"#;

const VAULT_REFUND_WAKEUP_CHANNEL: &str = "router_vault_refund_wakeups";

#[derive(Clone)]
pub struct VaultRepository {
    pool: PgPool,
}

impl VaultRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(&self, vault: &DepositVault) -> RouterServerResult<()> {
        self.create_internal(vault, None, "vault.create").await
    }

    pub async fn create_and_attach_order(
        &self,
        vault: &DepositVault,
        order_id: Uuid,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<()> {
        self.create_internal(
            vault,
            Some((order_id, updated_at)),
            "vault.create_and_attach_order",
        )
        .await
    }

    async fn create_internal(
        &self,
        vault: &DepositVault,
        order_attachment: Option<(Uuid, DateTime<Utc>)>,
        metric_name: &'static str,
    ) -> RouterServerResult<()> {
        let action =
            serde_json::to_value(&vault.action).map_err(|err| RouterServerError::InvalidData {
                message: format!("failed to encode vault action: {err}"),
            })?;
        let cancellation_commitment = decode_hex_32(&vault.cancellation_commitment)?;
        let funding_transfer_index =
            db_transfer_index_from_observation(vault.funding_observation.as_ref())?;

        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let custody_order_id = order_attachment
                .as_ref()
                .map(|(order_id, _)| *order_id)
                .or(vault.order_id);
            sqlx_core::query::query(
                r#"
                INSERT INTO custody_vaults (
                    id,
                    order_id,
                    role,
                    visibility,
                    chain_id,
                    asset_id,
                    address,
                    control_type,
                    derivation_salt,
                    signer_ref,
                    status,
                    metadata_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14
                )
                "#,
            )
            .bind(vault.id)
            .bind(custody_order_id)
            .bind(CustodyVaultRole::SourceDeposit.to_db_string())
            .bind(CustodyVaultVisibility::UserFacing.to_db_string())
            .bind(vault.deposit_asset.chain.as_str())
            .bind(vault.deposit_asset.asset.as_str())
            .bind(&vault.deposit_vault_address)
            .bind(CustodyVaultControlType::RouterDerivedKey.to_db_string())
            .bind(&vault.deposit_vault_salt[..])
            .bind(format!("deposit_vault:{}", vault.id))
            .bind(CustodyVaultStatus::Active.to_db_string())
            .bind(json!({
                "deposit_vault_id": vault.id,
                "role_reason": "user_funding_source"
            }))
            .bind(vault.created_at)
            .bind(vault.updated_at)
            .execute(&mut *tx)
            .await?;

            sqlx_core::query::query(
                r#"
                INSERT INTO deposit_vaults (
                    id,
                    action,
                    metadata,
                    recovery_address,
                    cancellation_commitment,
                    cancel_after,
                    status,
                    refund_requested_at,
                    refunded_at,
                    refund_tx_hash,
                    last_refund_error,
                    funding_tx_hash,
                    funding_sender_address,
                    funding_sender_addresses,
                    funding_recipient_address,
                    funding_transfer_index,
                    funding_observed_amount,
                    funding_confirmation_state,
                    funding_observed_at,
                    funding_evidence_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    $20, $21, $22
                )
                "#,
            )
            .bind(vault.id)
            .bind(action)
            .bind(vault.metadata.clone())
            .bind(&vault.recovery_address)
            .bind(&cancellation_commitment[..])
            .bind(vault.cancel_after)
            .bind(vault.status.to_db_string())
            .bind(vault.refund_requested_at)
            .bind(vault.refunded_at)
            .bind(vault.refund_tx_hash.clone())
            .bind(vault.last_refund_error.clone())
            .bind(
                vault
                    .funding_observation
                    .as_ref()
                    .and_then(|observation| observation.tx_hash.clone()),
            )
            .bind(
                vault
                    .funding_observation
                    .as_ref()
                    .and_then(|observation| observation.sender_address.clone()),
            )
            .bind(json!(vault
                .funding_observation
                .as_ref()
                .map(|observation| observation.sender_addresses.clone())
                .unwrap_or_default()))
            .bind(
                vault
                    .funding_observation
                    .as_ref()
                    .and_then(|observation| observation.recipient_address.clone()),
            )
            .bind(funding_transfer_index)
            .bind(
                vault
                    .funding_observation
                    .as_ref()
                    .and_then(|observation| observation.observed_amount.clone()),
            )
            .bind(
                vault
                    .funding_observation
                    .as_ref()
                    .and_then(|observation| observation.confirmation_state.clone()),
            )
            .bind(
                vault
                    .funding_observation
                    .as_ref()
                    .and_then(|observation| observation.observed_at),
            )
            .bind(
                vault
                    .funding_observation
                    .as_ref()
                    .map(|observation| observation.evidence.clone())
                    .unwrap_or_else(|| json!({})),
            )
            .bind(vault.created_at)
            .bind(vault.updated_at)
            .execute(&mut *tx)
            .await?;

            if let Some((order_id, updated_at)) = order_attachment {
                let attached = sqlx_core::query::query(
                    r#"
                    UPDATE router_orders
                    SET
                        funding_vault_id = $2,
                        status = 'pending_funding',
                        updated_at = $3
                    WHERE id = $1
                      AND funding_vault_id IS NULL
                      AND status = 'quoted'
                    RETURNING id
                    "#,
                )
                .bind(order_id)
                .bind(vault.id)
                .bind(updated_at)
                .fetch_optional(&mut *tx)
                .await?;

                if attached.is_none() {
                    return Err(RouterServerError::Validation {
                        message: format!("order {order_id} is no longer attachable"),
                    });
                }

                sqlx_core::query::query(
                    r#"
                    INSERT INTO order_execution_steps (
                        id,
                        order_id,
                        step_index,
                        step_type,
                        provider,
                        status,
                        input_chain_id,
                        input_asset_id,
                        details_json,
                        request_json,
                        response_json,
                        error_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8,
                        $9, $10, $11, $12, $13, $14
                    )
                    "#,
                )
                .bind(Uuid::now_v7())
                .bind(order_id)
                .bind(0_i32)
                .bind(OrderExecutionStepType::WaitForDeposit.to_db_string())
                .bind("internal")
                .bind(OrderExecutionStepStatus::Waiting.to_db_string())
                .bind(vault.deposit_asset.chain.as_str())
                .bind(vault.deposit_asset.asset.as_str())
                .bind(json!({
                    "vault_id": vault.id,
                    "custody_vault_id": vault.id,
                    "custody_vault_role": CustodyVaultRole::SourceDeposit.to_db_string()
                }))
                .bind(json!({}))
                .bind(json!({}))
                .bind(json!({}))
                .bind(updated_at)
                .bind(updated_at)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok::<(), RouterServerError>(())
        }
        .await;
        telemetry::record_db_query(metric_name, result.is_ok(), started.elapsed());
        result?;

        Ok(())
    }

    pub async fn get(&self, id: Uuid) -> RouterServerResult<DepositVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            "SELECT {SELECT_COLUMNS} FROM {SELECT_FROM} WHERE dv.id = $1"
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("vault.get", result.is_ok(), started.elapsed());
        let row = result?;

        self.map_row(&row)
    }

    pub async fn find_pending_funding_without_observation(
        &self,
        limit: i64,
    ) -> RouterServerResult<Vec<DepositVault>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {SELECT_COLUMNS}
            FROM {SELECT_FROM}
            WHERE dv.status = 'pending_funding'
              AND dv.funding_observed_amount IS NULL
            ORDER BY dv.created_at ASC, dv.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.find_pending_funding_without_observation",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_row(row)).collect()
    }

    pub async fn transition_status(
        &self,
        id: Uuid,
        from_status: DepositVaultStatus,
        to_status: DepositVaultStatus,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<DepositVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                status = $2,
                updated_at = $4
            WHERE id = $1
              AND status = $3
            RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(to_status.to_db_string())
        .bind(from_status.to_db_string())
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("vault.transition_status", result.is_ok(), started.elapsed());
        let row = result?;

        self.map_row(&row)
    }

    pub async fn mark_funded_with_observation(
        &self,
        id: Uuid,
        observation: &DepositVaultFundingObservation,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<DepositVault> {
        let funding_transfer_index = db_transfer_index_from_observation(Some(observation))?;
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                status = 'funded',
                funding_tx_hash = $2,
                funding_sender_address = $3,
                funding_sender_addresses = $4,
                funding_recipient_address = $5,
                funding_transfer_index = $6,
                funding_observed_amount = $7,
                funding_confirmation_state = $8,
                funding_observed_at = $9,
                funding_evidence_json = $10,
                updated_at = $11
            WHERE id = $1
              AND status = 'pending_funding'
            RETURNING *
            ),
            completed_wait_step AS (
            UPDATE order_execution_steps step
            SET
                status = 'completed',
                response_json = CASE
                    WHEN step.response_json = '{{}}'::jsonb THEN jsonb_strip_nulls(jsonb_build_object(
                        'reason', 'funding_vault_funded',
                        'tx_hash', $2,
                        'sender_address', $3,
                        'sender_addresses', $4::jsonb,
                        'recipient_address', $5,
                        'transfer_index', $6,
                        'vout', $6,
                        'amount', $7,
                        'confirmation_state', $8,
                        'observed_at', $9
                    ))
                    ELSE step.response_json
                END,
                tx_hash = COALESCE($2, step.tx_hash),
                completed_at = COALESCE(step.completed_at, $9, $11),
                updated_at = $11
            FROM updated
            JOIN custody_vaults cv ON cv.id = updated.id
            WHERE step.order_id = cv.order_id
              AND step.step_type = 'wait_for_deposit'
              AND step.status = 'waiting'
            RETURNING step.id
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(observation.tx_hash.clone())
        .bind(observation.sender_address.clone())
        .bind(json!(observation.sender_addresses.clone()))
        .bind(observation.recipient_address.clone())
        .bind(funding_transfer_index)
        .bind(observation.observed_amount.clone())
        .bind(observation.confirmation_state.clone())
        .bind(observation.observed_at)
        .bind(observation.evidence.clone())
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.mark_funded_with_observation",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_row(&row)
    }

    pub async fn record_refunding_observation(
        &self,
        id: Uuid,
        observation: &DepositVaultFundingObservation,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<DepositVault> {
        let funding_transfer_index = db_transfer_index_from_observation(Some(observation))?;
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                funding_tx_hash = $2,
                funding_sender_address = $3,
                funding_sender_addresses = $4,
                funding_recipient_address = $5,
                funding_transfer_index = $6,
                funding_observed_amount = $7,
                funding_confirmation_state = $8,
                funding_observed_at = $9,
                funding_evidence_json = $10,
                updated_at = $11
            WHERE id = $1
              AND status = 'refunding'
              AND funding_evidence_json = '{{}}'::jsonb
            RETURNING *
            ),
            completed_wait_step AS (
            UPDATE order_execution_steps step
            SET
                status = 'completed',
                response_json = CASE
                    WHEN step.response_json = '{{}}'::jsonb THEN jsonb_strip_nulls(jsonb_build_object(
                        'reason', 'funding_vault_funded',
                        'tx_hash', $2,
                        'sender_address', $3,
                        'sender_addresses', $4::jsonb,
                        'recipient_address', $5,
                        'transfer_index', $6,
                        'vout', $6,
                        'amount', $7,
                        'confirmation_state', $8,
                        'observed_at', $9
                    ))
                    ELSE step.response_json
                END,
                tx_hash = COALESCE($2, step.tx_hash),
                completed_at = COALESCE(step.completed_at, $9, $11),
                updated_at = $11
            FROM updated
            JOIN custody_vaults cv ON cv.id = updated.id
            WHERE step.order_id = cv.order_id
              AND step.step_type = 'wait_for_deposit'
              AND step.status = 'waiting'
            RETURNING step.id
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(observation.tx_hash.clone())
        .bind(observation.sender_address.clone())
        .bind(json!(observation.sender_addresses.clone()))
        .bind(observation.recipient_address.clone())
        .bind(funding_transfer_index)
        .bind(observation.observed_amount.clone())
        .bind(observation.confirmation_state.clone())
        .bind(observation.observed_at)
        .bind(observation.evidence.clone())
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.record_refunding_observation",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_row(&row)
    }

    pub async fn create_funding_hint(
        &self,
        hint: &DepositVaultFundingHint,
    ) -> RouterServerResult<DepositVaultFundingHint> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH upserted AS (
                INSERT INTO deposit_vault_funding_hints (
                    id,
                    vault_id,
                    source,
                    hint_kind,
                    evidence_json,
                    status,
                    idempotency_key,
                    error_json,
                    claimed_at,
                    processed_at,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (source, idempotency_key) WHERE idempotency_key IS NOT NULL
                DO UPDATE SET
                    vault_id = EXCLUDED.vault_id,
                    hint_kind = EXCLUDED.hint_kind,
                    evidence_json = EXCLUDED.evidence_json,
                    status = EXCLUDED.status,
                    error_json = EXCLUDED.error_json,
                    claimed_at = NULL,
                    processed_at = NULL,
                    updated_at = EXCLUDED.updated_at
                WHERE deposit_vault_funding_hints.source = $13
                  AND deposit_vault_funding_hints.status IN ('failed', 'ignored')
                RETURNING {FUNDING_HINT_SELECT_COLUMNS}
            )
            SELECT {FUNDING_HINT_SELECT_COLUMNS}
            FROM upserted
            UNION ALL
            SELECT {FUNDING_HINT_RETURNING_COLUMNS}
            FROM deposit_vault_funding_hints hints
            WHERE hints.source = $3
              AND hints.idempotency_key = $7
              AND NOT EXISTS (SELECT 1 FROM upserted)
            LIMIT 1
            "#
        ))
        .bind(hint.id)
        .bind(hint.vault_id)
        .bind(&hint.source)
        .bind(hint.hint_kind.to_db_string())
        .bind(hint.evidence.clone())
        .bind(hint.status.to_db_string())
        .bind(hint.idempotency_key.clone())
        .bind(hint.error.clone())
        .bind(hint.claimed_at)
        .bind(hint.processed_at)
        .bind(hint.created_at)
        .bind(hint.updated_at)
        .bind(SAURON_DETECTOR_HINT_SOURCE)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.create_funding_hint",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;
        let hint = self.map_funding_hint_row(&row)?;

        let started = Instant::now();
        let result = sqlx_core::query::query("SELECT pg_notify('router_vault_funding_hints', $1)")
            .bind(hint.id.to_string())
            .execute(&self.pool)
            .await;
        telemetry::record_db_query(
            "vault.notify_funding_hint",
            result.is_ok(),
            started.elapsed(),
        );
        result?;

        Ok(hint)
    }

    pub async fn claim_pending_funding_hints(
        &self,
        limit: i64,
        now: DateTime<Utc>,
    ) -> RouterServerResult<Vec<DepositVaultFundingHint>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH pending_candidates AS (
                SELECT id, created_at
                FROM deposit_vault_funding_hints
                WHERE status = 'pending'
                ORDER BY created_at ASC, id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ),
            processing_candidates AS (
                SELECT id, created_at
                FROM deposit_vault_funding_hints
                WHERE status = 'processing'
                  AND claimed_at < $2 - INTERVAL '5 minutes'
                ORDER BY claimed_at ASC, created_at ASC, id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ),
            candidates AS (
                SELECT id
                FROM (
                    SELECT id, created_at FROM pending_candidates
                    UNION ALL
                    SELECT id, created_at FROM processing_candidates
                ) claimable
                ORDER BY created_at ASC, id ASC
                LIMIT $1
            )
            UPDATE deposit_vault_funding_hints hints
            SET
                status = 'processing',
                claimed_at = $2,
                updated_at = $2
            FROM candidates
            WHERE hints.id = candidates.id
            RETURNING {FUNDING_HINT_RETURNING_COLUMNS}
            "#
        ))
        .bind(limit)
        .bind(now)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.claim_pending_funding_hints",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_funding_hint_row(row))
            .collect()
    }

    pub async fn complete_funding_hint(
        &self,
        id: Uuid,
        claimed_at: Option<DateTime<Utc>>,
        status: ProviderOperationHintStatus,
        error: serde_json::Value,
        now: DateTime<Utc>,
    ) -> RouterServerResult<DepositVaultFundingHint> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE deposit_vault_funding_hints
            SET
                status = $2,
                error_json = $3,
                processed_at = $4,
                updated_at = $4
            WHERE id = $1
              AND status = 'processing'
              AND claimed_at IS NOT DISTINCT FROM $5::timestamptz
            RETURNING {FUNDING_HINT_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(status.to_db_string())
        .bind(error)
        .bind(now)
        .bind(claimed_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.complete_funding_hint",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_funding_hint_row(&row)
    }

    pub async fn request_refund(
        &self,
        id: Uuid,
        requested_at: DateTime<Utc>,
    ) -> RouterServerResult<DepositVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                status = 'refunding',
                refund_requested_at = COALESCE(refund_requested_at, $2),
                last_refund_error = CASE
                    WHEN status = 'refunding' THEN last_refund_error
                    ELSE NULL
                END,
                refund_next_attempt_at = CASE
                    WHEN status = 'refunding' THEN refund_next_attempt_at
                    ELSE $2
                END,
                updated_at = $2
            WHERE id = $1
              AND status IN (
                  'pending_funding',
                  'funded',
                  'executing',
                  'refund_required',
                  'refunding'
              )
            RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(requested_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("vault.request_refund", result.is_ok(), started.elapsed());
        let row = result?;
        let vault = self.map_row(&row)?;

        let started = Instant::now();
        let result = sqlx_core::query::query("SELECT pg_notify($1, $2)")
            .bind(VAULT_REFUND_WAKEUP_CHANNEL)
            .bind(vault.id.to_string())
            .execute(&self.pool)
            .await;
        telemetry::record_db_query(
            "vault.notify_refund_wakeup",
            result.is_ok(),
            started.elapsed(),
        );
        result?;

        Ok(vault)
    }

    pub async fn claim_refund(
        &self,
        id: Uuid,
        now: DateTime<Utc>,
        claimed_until: DateTime<Utc>,
        worker_id: &str,
    ) -> RouterServerResult<Option<DepositVault>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                refund_claimed_by = $2,
                refund_claimed_until = $3,
                last_refund_attempt_at = $4,
                refund_attempt_count = refund_attempt_count + 1,
                updated_at = $4
            WHERE id = $1
              AND status = 'refunding'
              AND (
                refund_claimed_until IS NULL
                OR refund_claimed_until <= $4
                OR refund_claimed_by = $2
              )
            RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(worker_id)
        .bind(claimed_until)
        .bind(now)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query("vault.claim_refund", result.is_ok(), started.elapsed());
        let row = result?;

        row.map(|row| self.map_row(&row)).transpose()
    }

    pub async fn claim_due_for_timeout(
        &self,
        now: DateTime<Utc>,
        claimed_until: DateTime<Utc>,
        worker_id: &str,
        limit: i64,
    ) -> RouterServerResult<Vec<DepositVault>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH candidates AS (
                SELECT dv.id
                FROM deposit_vaults dv
                JOIN custody_vaults cv ON cv.id = dv.id
                WHERE dv.cancel_after <= $1
                  AND dv.status = 'funded'
                  AND cv.order_id IS NULL
                ORDER BY dv.cancel_after ASC
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE deposit_vaults
                SET
                    status = 'refunding',
                    refund_requested_at = COALESCE(refund_requested_at, $1),
                    last_refund_error = NULL,
                    refund_next_attempt_at = $1,
                    refund_claimed_by = $3,
                    refund_claimed_until = $2,
                    last_refund_attempt_at = $1,
                    refund_attempt_count = refund_attempt_count + 1,
                    updated_at = $1
                WHERE id IN (SELECT id FROM candidates)
                RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(now)
        .bind(claimed_until)
        .bind(worker_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.claim_due_for_timeout",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_row(row)).collect()
    }

    pub async fn claim_refunding(
        &self,
        now: DateTime<Utc>,
        claimed_until: DateTime<Utc>,
        worker_id: &str,
        limit: i64,
    ) -> RouterServerResult<Vec<DepositVault>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH candidates AS (
                SELECT id
                FROM deposit_vaults
                WHERE status = 'refunding'
                  AND (refund_next_attempt_at IS NULL OR refund_next_attempt_at <= $1)
                  AND (refund_claimed_until IS NULL OR refund_claimed_until <= $1)
                ORDER BY updated_at ASC
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE deposit_vaults
                SET
                    refund_claimed_by = $3,
                    refund_claimed_until = $2,
                    last_refund_attempt_at = $1,
                    refund_attempt_count = refund_attempt_count + 1,
                    updated_at = $1
                WHERE id IN (SELECT id FROM candidates)
                RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(now)
        .bind(claimed_until)
        .bind(worker_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query("vault.claim_refunding", result.is_ok(), started.elapsed());
        let rows = result?;

        rows.iter().map(|row| self.map_row(row)).collect()
    }

    pub async fn next_refund_due_at(
        &self,
        now: DateTime<Utc>,
    ) -> RouterServerResult<Option<DateTime<Utc>>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            SELECT MIN(due_at) AS due_at
            FROM (
                SELECT cancel_after AS due_at
                FROM deposit_vaults dv
                JOIN custody_vaults cv ON cv.id = dv.id
                WHERE dv.status = 'funded'
                  AND cv.order_id IS NULL

                UNION ALL

                SELECT GREATEST(
                    COALESCE(refund_next_attempt_at, $1),
                    COALESCE(refund_claimed_until, $1)
                ) AS due_at
                FROM deposit_vaults
                WHERE status = 'refunding'
            ) due_refunds
            "#,
        )
        .bind(now)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.next_refund_due_at",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        Ok(row.try_get("due_at")?)
    }

    pub async fn mark_refunded(
        &self,
        id: Uuid,
        refunded_at: DateTime<Utc>,
        refund_tx_hash: &str,
        claimed_by: &str,
        claimed_until: DateTime<Utc>,
    ) -> RouterServerResult<DepositVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                status = 'refunded',
                refunded_at = $2,
                refund_tx_hash = $3,
                last_refund_error = NULL,
                refund_next_attempt_at = NULL,
                refund_claimed_by = NULL,
                refund_claimed_until = NULL,
                updated_at = $2
            WHERE id = $1
              AND status = 'refunding'
              AND refund_claimed_by = $4
              AND refund_claimed_until IS NOT DISTINCT FROM $5::timestamptz
            RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(refunded_at)
        .bind(refund_tx_hash)
        .bind(claimed_by)
        .bind(claimed_until)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("vault.mark_refunded", result.is_ok(), started.elapsed());
        let row = result?;

        self.map_row(&row)
    }

    pub async fn mark_refund_error(
        &self,
        id: Uuid,
        updated_at: DateTime<Utc>,
        next_attempt_at: DateTime<Utc>,
        last_refund_error: &str,
        claimed_by: &str,
        claimed_until: DateTime<Utc>,
    ) -> RouterServerResult<DepositVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                status = 'refunding',
                last_refund_error = $3,
                refund_next_attempt_at = $4,
                refund_claimed_by = NULL,
                refund_claimed_until = NULL,
                updated_at = $2
            WHERE id = $1
              AND status = 'refunding'
              AND refund_claimed_by = $5
              AND refund_claimed_until IS NOT DISTINCT FROM $6::timestamptz
            RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(updated_at)
        .bind(last_refund_error)
        .bind(next_attempt_at)
        .bind(claimed_by)
        .bind(claimed_until)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("vault.mark_refund_error", result.is_ok(), started.elapsed());
        let row = result?;

        self.map_row(&row)
    }

    pub async fn mark_refund_manual_intervention_required(
        &self,
        id: Uuid,
        updated_at: DateTime<Utc>,
        last_refund_error: &str,
        claimed_by: &str,
        claimed_until: DateTime<Utc>,
    ) -> RouterServerResult<DepositVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
            UPDATE deposit_vaults
            SET
                status = 'refund_manual_intervention_required',
                last_refund_error = $3,
                refund_next_attempt_at = NULL,
                refund_claimed_by = NULL,
                refund_claimed_until = NULL,
                updated_at = $2
            WHERE id = $1
              AND status = 'refunding'
              AND refund_claimed_by = $4
              AND refund_claimed_until IS NOT DISTINCT FROM $5::timestamptz
            RETURNING *
            )
            SELECT {SELECT_COLUMNS}
            FROM updated dv
            JOIN custody_vaults cv ON cv.id = dv.id
            "#
        ))
        .bind(id)
        .bind(updated_at)
        .bind(last_refund_error)
        .bind(claimed_by)
        .bind(claimed_until)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "vault.mark_refund_manual_intervention_required",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_row(&row)
    }

    fn map_funding_hint_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterServerResult<DepositVaultFundingHint> {
        let hint_kind = row.get::<String, _>("hint_kind");
        let hint_kind = ProviderOperationHintKind::from_db_string(&hint_kind).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported vault funding hint kind: {hint_kind}"),
            }
        })?;
        let status = row.get::<String, _>("status");
        let status = ProviderOperationHintStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported vault funding hint status: {status}"),
            }
        })?;

        Ok(DepositVaultFundingHint {
            id: row.get("id"),
            vault_id: row.get("vault_id"),
            source: row.get("source"),
            hint_kind,
            evidence: row.get("evidence_json"),
            status,
            idempotency_key: row.get("idempotency_key"),
            error: row.get("error_json"),
            claimed_at: row.get("claimed_at"),
            processed_at: row.get("processed_at"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_row(&self, row: &sqlx_postgres::PgRow) -> RouterServerResult<DepositVault> {
        let deposit_chain = row.get::<String, _>("deposit_chain_id");
        let deposit_chain =
            ChainId::parse(deposit_chain).map_err(|err| RouterServerError::InvalidData {
                message: format!("unsupported deposit chain id: {err}"),
            })?;

        let deposit_asset = row.get::<String, _>("deposit_asset_id");
        let deposit_asset =
            AssetId::parse(deposit_asset).map_err(|err| RouterServerError::InvalidData {
                message: format!("unsupported deposit asset id: {err}"),
            })?;

        let action: serde_json::Value = row.get("action");
        let action: VaultAction =
            serde_json::from_value(action).map_err(|err| RouterServerError::InvalidData {
                message: format!("invalid action payload in database: {err}"),
            })?;

        let status = row.get::<String, _>("status");
        let status = DepositVaultStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported vault status: {status}"),
            }
        })?;

        let salt = row.get::<Vec<u8>, _>("deposit_vault_salt");
        let deposit_vault_salt: [u8; 32] =
            salt.try_into()
                .map_err(|_| RouterServerError::InvalidData {
                    message: "deposit_vault_salt must be 32 bytes".to_string(),
                })?;

        let cancellation_commitment = row.get::<Vec<u8>, _>("cancellation_commitment");
        let cancellation_commitment: [u8; 32] =
            cancellation_commitment
                .try_into()
                .map_err(|_| RouterServerError::InvalidData {
                    message: "cancellation_commitment must be 32 bytes".to_string(),
                })?;

        let funding_sender_addresses_json: serde_json::Value = row.get("funding_sender_addresses");
        let funding_sender_addresses = serde_json::from_value::<Vec<String>>(
            funding_sender_addresses_json.clone(),
        )
        .map_err(|err| RouterServerError::InvalidData {
            message: format!("invalid funding sender addresses payload: {err}"),
        })?;
        let funding_evidence: serde_json::Value = row.get("funding_evidence_json");
        let funding_observation = {
            let tx_hash = row.get::<Option<String>, _>("funding_tx_hash");
            let sender_address = row.get::<Option<String>, _>("funding_sender_address");
            let recipient_address = row.get::<Option<String>, _>("funding_recipient_address");
            let transfer_index = transfer_index_from_db(row.get("funding_transfer_index"))?;
            let observed_amount = row.get::<Option<String>, _>("funding_observed_amount");
            let confirmation_state = row.get::<Option<String>, _>("funding_confirmation_state");
            let observed_at = row.get::<Option<DateTime<Utc>>, _>("funding_observed_at");
            if tx_hash.is_some()
                || sender_address.is_some()
                || !funding_sender_addresses.is_empty()
                || recipient_address.is_some()
                || observed_amount.is_some()
            {
                Some(DepositVaultFundingObservation {
                    tx_hash,
                    sender_address,
                    sender_addresses: funding_sender_addresses,
                    recipient_address,
                    transfer_index,
                    observed_amount,
                    confirmation_state,
                    observed_at,
                    evidence: funding_evidence,
                })
            } else {
                None
            }
        };

        Ok(DepositVault {
            id: row.get("id"),
            order_id: row.get("order_id"),
            deposit_asset: DepositAsset {
                chain: deposit_chain,
                asset: deposit_asset,
            },
            action,
            metadata: row.get("metadata"),
            deposit_vault_salt,
            deposit_vault_address: row.get("deposit_vault_address"),
            recovery_address: row.get("recovery_address"),
            cancellation_commitment: format!("0x{}", alloy::hex::encode(cancellation_commitment)),
            cancel_after: row.get("cancel_after"),
            status,
            refund_requested_at: row.get("refund_requested_at"),
            refunded_at: row.get("refunded_at"),
            refund_tx_hash: row.get("refund_tx_hash"),
            last_refund_error: row.get("last_refund_error"),
            funding_observation,
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }
}

fn decode_hex_32(value: &str) -> RouterServerResult<[u8; 32]> {
    let stripped = value.strip_prefix("0x").unwrap_or(value);
    let bytes = alloy::hex::decode(stripped).map_err(|err| RouterServerError::Validation {
        message: format!("invalid 32-byte hex value: {err}"),
    })?;

    bytes.try_into().map_err(|_| RouterServerError::Validation {
        message: "expected 32-byte hex value".to_string(),
    })
}

fn db_transfer_index_from_observation(
    observation: Option<&DepositVaultFundingObservation>,
) -> RouterServerResult<Option<i64>> {
    observation
        .and_then(|observation| observation.transfer_index)
        .map(db_transfer_index_from_u64)
        .transpose()
}

fn db_transfer_index_from_u64(index: u64) -> RouterServerResult<i64> {
    i64::try_from(index).map_err(|_| RouterServerError::InvalidData {
        message: format!("funding transfer index {index} exceeds Postgres bigint range"),
    })
}

fn transfer_index_from_db(index: Option<i64>) -> RouterServerResult<Option<u64>> {
    index
        .map(|index| {
            u64::try_from(index).map_err(|_| RouterServerError::InvalidData {
                message: format!("funding transfer index {index} is negative"),
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_transfer_index_rejects_values_above_signed_bigint() {
        assert_eq!(
            db_transfer_index_from_u64(i64::MAX as u64).unwrap(),
            i64::MAX
        );
        assert!(db_transfer_index_from_u64(i64::MAX as u64 + 1).is_err());
    }

    #[test]
    fn transfer_index_from_db_rejects_negative_values() {
        assert_eq!(transfer_index_from_db(Some(0)).unwrap(), Some(0));
        assert_eq!(
            transfer_index_from_db(Some(i64::MAX)).unwrap(),
            Some(i64::MAX as u64)
        );
        assert!(transfer_index_from_db(Some(-1)).is_err());
    }
}
