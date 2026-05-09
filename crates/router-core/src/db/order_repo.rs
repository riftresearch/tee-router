use crate::{
    error::{RouterCoreError, RouterCoreResult},
    models::{
        CustodyVault, CustodyVaultControlType, CustodyVaultRole, CustodyVaultStatus,
        CustodyVaultVisibility, DepositVaultStatus, LimitOrderAction, LimitOrderQuote,
        LimitOrderResidualPolicy, MarketOrderKind, MarketOrderKindType, MarketOrderQuote,
        OrderExecutionAttempt, OrderExecutionAttemptKind, OrderExecutionAttemptStatus,
        OrderExecutionLeg, OrderExecutionStep, OrderExecutionStepStatus, OrderExecutionStepType,
        OrderProviderAddress, OrderProviderOperation, OrderProviderOperationHint,
        ProviderAddressRole, ProviderOperationHintKind, ProviderOperationHintStatus,
        ProviderOperationStatus, ProviderOperationType, RouterOrder, RouterOrderAction,
        RouterOrderQuote, RouterOrderStatus, RouterOrderType,
        PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE,
    },
    protocol::{AssetId, ChainId, DepositAsset},
    telemetry,
};
use chrono::{DateTime, Utc};
use sqlx_core::row::Row;
use sqlx_postgres::{PgPool, Postgres};
use std::{collections::HashMap, time::Instant};
use uuid::Uuid;

const ORDER_SELECT_COLUMNS: &str = r#"
    ro.id,
    ro.order_type,
    ro.status,
    ro.funding_vault_id,
    ro.source_chain_id,
    ro.source_asset_id,
    ro.destination_chain_id,
    ro.destination_asset_id,
    ro.recipient_address,
    ro.refund_address,
    ro.action_timeout_at,
    ro.idempotency_key,
    ro.workflow_trace_id,
    ro.workflow_parent_span_id,
    ro.created_at,
    ro.updated_at,
    moa.order_kind AS market_order_kind,
    moa.amount_in AS market_order_amount_in,
    moa.min_amount_out AS market_order_min_amount_out,
    moa.amount_out AS market_order_amount_out,
    moa.max_amount_in AS market_order_max_amount_in,
    moa.slippage_bps AS market_order_slippage_bps,
    loa.input_amount AS limit_order_input_amount,
    loa.output_amount AS limit_order_output_amount,
    loa.residual_policy AS limit_order_residual_policy
"#;

const QUOTE_SELECT_COLUMNS: &str = r#"
    id,
    order_id,
    source_chain_id,
    source_asset_id,
    destination_chain_id,
    destination_asset_id,
    recipient_address,
    provider_id,
    order_kind,
    amount_in,
    amount_out,
    min_amount_out,
    max_amount_in,
    slippage_bps,
    provider_quote,
    usd_valuation_json,
    expires_at,
    created_at
"#;

const LIMIT_QUOTE_SELECT_COLUMNS: &str = r#"
    id,
    order_id,
    source_chain_id,
    source_asset_id,
    destination_chain_id,
    destination_asset_id,
    recipient_address,
    provider_id,
    input_amount,
    output_amount,
    residual_policy,
    provider_quote,
    usd_valuation_json,
    expires_at,
    created_at
"#;

const CUSTODY_VAULT_SELECT_COLUMNS: &str = r#"
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
"#;

const PROVIDER_OPERATION_SELECT_COLUMNS: &str = r#"
    id,
    order_id,
    execution_attempt_id,
    execution_step_id,
    provider,
    operation_type,
    provider_ref,
    status,
    request_json,
    response_json,
    observed_state_json,
    created_at,
    updated_at
"#;

const PROVIDER_ADDRESS_SELECT_COLUMNS: &str = r#"
    id,
    order_id,
    execution_step_id,
    provider_operation_id,
    provider,
    role,
    chain_id,
    asset_id,
    address,
    memo,
    expires_at,
    metadata_json,
    created_at,
    updated_at
"#;

const PROVIDER_OPERATION_HINT_SELECT_COLUMNS: &str = r#"
    id,
    provider_operation_id,
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

const PROVIDER_OPERATION_HINT_RETURNING_COLUMNS: &str = r#"
    hints.id,
    hints.provider_operation_id,
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

const EXECUTION_STEP_SELECT_COLUMNS: &str = r#"
    id,
    order_id,
    execution_attempt_id,
    execution_leg_id,
    transition_decl_id,
    step_index,
    step_type,
    provider,
    status,
    input_chain_id,
    input_asset_id,
    output_chain_id,
    output_asset_id,
    amount_in,
    min_amount_out,
    tx_hash,
    provider_ref,
    idempotency_key,
    attempt_count,
    next_attempt_at,
    started_at,
    completed_at,
    details_json,
    request_json,
    response_json,
    error_json,
    usd_valuation_json,
    created_at,
    updated_at
"#;

const EXECUTION_LEG_SELECT_COLUMNS: &str = r#"
    id,
    order_id,
    execution_attempt_id,
    transition_decl_id,
    leg_index,
    leg_type,
    provider,
    status,
    input_chain_id,
    input_asset_id,
    output_chain_id,
    output_asset_id,
    amount_in,
    expected_amount_out,
    min_amount_out,
    actual_amount_in,
    actual_amount_out,
    started_at,
    completed_at,
    provider_quote_expires_at,
    details_json,
    usd_valuation_json,
    created_at,
    updated_at
"#;

const EXECUTION_LEG_RETURNING_COLUMNS: &str = r#"
    leg.id,
    leg.order_id,
    leg.execution_attempt_id,
    leg.transition_decl_id,
    leg.leg_index,
    leg.leg_type,
    leg.provider,
    leg.status,
    leg.input_chain_id,
    leg.input_asset_id,
    leg.output_chain_id,
    leg.output_asset_id,
    leg.amount_in,
    leg.expected_amount_out,
    leg.min_amount_out,
    leg.actual_amount_in,
    leg.actual_amount_out,
    leg.started_at,
    leg.completed_at,
    leg.provider_quote_expires_at,
    leg.details_json,
    leg.usd_valuation_json,
    leg.created_at,
    leg.updated_at
"#;

const EXECUTION_ATTEMPT_SELECT_COLUMNS: &str = r#"
    id,
    order_id,
    attempt_index,
    attempt_kind,
    status,
    trigger_step_id,
    trigger_provider_operation_id,
    failure_reason_json,
    input_custody_snapshot_json,
    superseded_by_attempt_id,
    superseded_reason_json,
    created_at,
    updated_at
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderStatusCount {
    pub status: RouterOrderStatus,
    pub order_count: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutionAttemptPlan {
    pub legs: Vec<OrderExecutionLeg>,
    pub steps: Vec<OrderExecutionStep>,
}

#[derive(Debug, Clone)]
pub struct RefreshedExecutionAttemptPlan {
    pub legs: Vec<OrderExecutionLeg>,
    pub steps: Vec<OrderExecutionStep>,
    pub failure_reason: serde_json::Value,
    pub superseded_reason: serde_json::Value,
    pub input_custody_snapshot: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct FundingVaultRefundAttemptPlan {
    pub funding_vault_id: Uuid,
    pub amount: String,
    pub failure_reason: serde_json::Value,
    pub input_custody_snapshot: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct ExecutionAttemptMaterializationRecord {
    pub order: RouterOrder,
    pub attempt: OrderExecutionAttempt,
    pub legs: Vec<OrderExecutionLeg>,
    pub steps: Vec<OrderExecutionStep>,
}

#[derive(Debug, Clone)]
pub struct PersistStepCompletionRecord {
    pub step_id: Uuid,
    pub operation: Option<OrderProviderOperation>,
    pub addresses: Vec<OrderProviderAddress>,
    pub response: serde_json::Value,
    pub tx_hash: Option<String>,
    pub usd_valuation: serde_json::Value,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StepCompletionRecord {
    pub step: OrderExecutionStep,
    pub provider_operation_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct CompletedExecutionOrder {
    pub order: RouterOrder,
    pub attempt: OrderExecutionAttempt,
}

#[derive(Clone)]
pub struct OrderRepository {
    pool: PgPool,
}

impl OrderRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_market_order_quote(
        &self,
        quote: &MarketOrderQuote,
    ) -> RouterCoreResult<()> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            INSERT INTO market_order_quotes (
                id,
                order_id,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                provider_id,
                order_kind,
                amount_in,
                amount_out,
                min_amount_out,
                max_amount_in,
                slippage_bps,
                provider_quote,
                usd_valuation_json,
                expires_at,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
            )
            "#,
        )
        .bind(quote.id)
        .bind(quote.order_id)
        .bind(quote.source_asset.chain.as_str())
        .bind(quote.source_asset.asset.as_str())
        .bind(quote.destination_asset.chain.as_str())
        .bind(quote.destination_asset.asset.as_str())
        .bind(&quote.recipient_address)
        .bind(&quote.provider_id)
        .bind(quote.order_kind.to_db_string())
        .bind(&quote.amount_in)
        .bind(&quote.amount_out)
        .bind(quote.min_amount_out.clone())
        .bind(quote.max_amount_in.clone())
        .bind(optional_slippage_bps_i64(quote.slippage_bps, "quote")?)
        .bind(quote.provider_quote.clone())
        .bind(quote.usd_valuation.clone())
        .bind(quote.expires_at)
        .bind(quote.created_at)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_market_order_quote",
            result.is_ok(),
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    pub async fn create_limit_order_quote(&self, quote: &LimitOrderQuote) -> RouterCoreResult<()> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            INSERT INTO limit_order_quotes (
                id,
                order_id,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                provider_id,
                input_amount,
                output_amount,
                residual_policy,
                provider_quote,
                usd_valuation_json,
                expires_at,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15
            )
            "#,
        )
        .bind(quote.id)
        .bind(quote.order_id)
        .bind(quote.source_asset.chain.as_str())
        .bind(quote.source_asset.asset.as_str())
        .bind(quote.destination_asset.chain.as_str())
        .bind(quote.destination_asset.asset.as_str())
        .bind(&quote.recipient_address)
        .bind(&quote.provider_id)
        .bind(&quote.input_amount)
        .bind(&quote.output_amount)
        .bind(quote.residual_policy.to_db_string())
        .bind(quote.provider_quote.clone())
        .bind(quote.usd_valuation.clone())
        .bind(quote.expires_at)
        .bind(quote.created_at)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_limit_order_quote",
            result.is_ok(),
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    pub async fn create_market_order_from_quote(
        &self,
        order: &RouterOrder,
        quote_id: Uuid,
    ) -> RouterCoreResult<MarketOrderQuote> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let market_action = market_order_action_fields(&order.action)?;

            sqlx_core::query::query(
                r#"
                INSERT INTO router_orders (
                    id,
                    order_type,
                    status,
                    funding_vault_id,
                    source_chain_id,
                    source_asset_id,
                    destination_chain_id,
                    destination_asset_id,
                    recipient_address,
                    refund_address,
                    action_timeout_at,
                    idempotency_key,
                    workflow_trace_id,
                    workflow_parent_span_id,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15, $16
                )
                "#,
            )
            .bind(order.id)
            .bind(order.order_type.to_db_string())
            .bind(order.status.to_db_string())
            .bind(order.funding_vault_id)
            .bind(order.source_asset.chain.as_str())
            .bind(order.source_asset.asset.as_str())
            .bind(order.destination_asset.chain.as_str())
            .bind(order.destination_asset.asset.as_str())
            .bind(&order.recipient_address)
            .bind(&order.refund_address)
            .bind(order.action_timeout_at)
            .bind(order.idempotency_key.clone())
            .bind(&order.workflow_trace_id)
            .bind(&order.workflow_parent_span_id)
            .bind(order.created_at)
            .bind(order.updated_at)
            .execute(&mut *tx)
            .await?;

            sqlx_core::query::query(
                r#"
                INSERT INTO market_order_actions (
                    order_id,
                    order_kind,
                    amount_in,
                    min_amount_out,
                    amount_out,
                    max_amount_in,
                    slippage_bps,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(order.id)
            .bind(market_action.order_kind.to_db_string())
            .bind(market_action.amount_in)
            .bind(market_action.min_amount_out)
            .bind(market_action.amount_out)
            .bind(market_action.max_amount_in)
            .bind(optional_slippage_bps_i64(
                market_action.slippage_bps,
                "order",
            )?)
            .bind(order.created_at)
            .bind(order.updated_at)
            .execute(&mut *tx)
            .await?;

            let row = sqlx_core::query::query(&format!(
                r#"
                UPDATE market_order_quotes
                SET order_id = $2
                WHERE id = $1
                  AND order_id IS NULL
                  AND expires_at > now()
                RETURNING {QUOTE_SELECT_COLUMNS}
                "#
            ))
            .bind(quote_id)
            .bind(order.id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(RouterCoreError::Conflict {
                message: format!("quote {quote_id} is no longer orderable"),
            })?;

            let quote = self.map_quote_row(&row)?;
            tx.commit().await?;
            Ok::<MarketOrderQuote, RouterCoreError>(quote)
        }
        .await;
        telemetry::record_db_query(
            "order.create_market_order_from_quote",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn create_limit_order_from_quote(
        &self,
        order: &RouterOrder,
        quote_id: Uuid,
    ) -> RouterCoreResult<LimitOrderQuote> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let limit_action = limit_order_action_fields(&order.action)?;

            sqlx_core::query::query(
                r#"
                INSERT INTO router_orders (
                    id,
                    order_type,
                    status,
                    funding_vault_id,
                    source_chain_id,
                    source_asset_id,
                    destination_chain_id,
                    destination_asset_id,
                    recipient_address,
                    refund_address,
                    action_timeout_at,
                    idempotency_key,
                    workflow_trace_id,
                    workflow_parent_span_id,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, $11, $12, $13, $14, $15, $16
                )
                "#,
            )
            .bind(order.id)
            .bind(order.order_type.to_db_string())
            .bind(order.status.to_db_string())
            .bind(order.funding_vault_id)
            .bind(order.source_asset.chain.as_str())
            .bind(order.source_asset.asset.as_str())
            .bind(order.destination_asset.chain.as_str())
            .bind(order.destination_asset.asset.as_str())
            .bind(&order.recipient_address)
            .bind(&order.refund_address)
            .bind(order.action_timeout_at)
            .bind(order.idempotency_key.clone())
            .bind(&order.workflow_trace_id)
            .bind(&order.workflow_parent_span_id)
            .bind(order.created_at)
            .bind(order.updated_at)
            .execute(&mut *tx)
            .await?;

            sqlx_core::query::query(
                r#"
                INSERT INTO limit_order_actions (
                    order_id,
                    input_amount,
                    output_amount,
                    residual_policy,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(order.id)
            .bind(limit_action.input_amount)
            .bind(limit_action.output_amount)
            .bind(limit_action.residual_policy.to_db_string())
            .bind(order.created_at)
            .bind(order.updated_at)
            .execute(&mut *tx)
            .await?;

            let row = sqlx_core::query::query(&format!(
                r#"
                UPDATE limit_order_quotes
                SET order_id = $2
                WHERE id = $1
                  AND order_id IS NULL
                  AND expires_at > now()
                RETURNING {LIMIT_QUOTE_SELECT_COLUMNS}
                "#
            ))
            .bind(quote_id)
            .bind(order.id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(RouterCoreError::Conflict {
                message: format!("quote {quote_id} is no longer orderable"),
            })?;

            let quote = self.map_limit_quote_row(&row)?;
            tx.commit().await?;
            Ok::<LimitOrderQuote, RouterCoreError>(quote)
        }
        .await;
        telemetry::record_db_query(
            "order.create_limit_order_from_quote",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn release_quote_and_delete_quoted_order(
        &self,
        order_id: Uuid,
        quote_id: Uuid,
    ) -> RouterCoreResult<()> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;

            let released_market_quote = sqlx_core::query::query(
                r#"
                UPDATE market_order_quotes
                SET order_id = NULL
                WHERE id = $1
                  AND order_id = $2
                "#,
            )
            .bind(quote_id)
            .bind(order_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            let released_limit_quote = sqlx_core::query::query(
                r#"
                UPDATE limit_order_quotes
                SET order_id = NULL
                WHERE id = $1
                  AND order_id = $2
                "#,
            )
            .bind(quote_id)
            .bind(order_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();

            let deleted_order = sqlx_core::query::query(
                r#"
                DELETE FROM router_orders
                WHERE id = $1
                  AND status = $2
                  AND funding_vault_id IS NULL
                "#,
            )
            .bind(order_id)
            .bind(RouterOrderStatus::Quoted.to_db_string())
            .execute(&mut *tx)
            .await?
            .rows_affected();

            if released_market_quote + released_limit_quote != 1 || deleted_order != 1 {
                return Err(RouterCoreError::Validation {
                    message: format!(
                        "order {order_id} could not be rolled back for quote {quote_id}"
                    ),
                });
            }

            tx.commit().await?;
            Ok::<(), RouterCoreError>(())
        }
        .await;
        telemetry::record_db_query(
            "order.release_quote_and_delete_quoted_order",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn get(&self, id: Uuid) -> RouterCoreResult<RouterOrder> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            WHERE ro.id = $1
            "#
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("order.get", result.is_ok(), started.elapsed());
        let row = result?;

        self.map_order_row(&row)
    }

    pub async fn count_in_progress_orders_by_status(
        &self,
    ) -> RouterCoreResult<Vec<OrderStatusCount>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            SELECT status, COUNT(*)::bigint AS order_count
            FROM router_orders
            WHERE status IN (
                'pending_funding',
                'funded',
                'executing',
                'refund_required',
                'refunding'
            )
            GROUP BY status
            "#,
        )
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.count_in_progress_orders_by_status",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| {
                let status: String = row.try_get("status")?;
                let status = RouterOrderStatus::from_db_string(&status).ok_or_else(|| {
                    RouterCoreError::InvalidData {
                        message: format!("unknown order status: {status}"),
                    }
                })?;
                let order_count: i64 = row.try_get("order_count")?;
                let order_count =
                    u64::try_from(order_count).map_err(|_| RouterCoreError::InvalidData {
                        message: format!(
                            "negative order status count for {}",
                            status.to_db_string()
                        ),
                    })?;
                Ok(OrderStatusCount {
                    status,
                    order_count,
                })
            })
            .collect()
    }

    pub async fn transition_status(
        &self,
        id: Uuid,
        from_status: RouterOrderStatus,
        to_status: RouterOrderStatus,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<RouterOrder> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
                UPDATE router_orders
                SET
                    status = $2,
                    updated_at = $4
                WHERE id = $1
                  AND status = $3
                RETURNING *
            )
            SELECT {ORDER_SELECT_COLUMNS}
            FROM updated ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            "#
        ))
        .bind(id)
        .bind(to_status.to_db_string())
        .bind(from_status.to_db_string())
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("order.transition_status", result.is_ok(), started.elapsed());
        let row = result?;

        self.map_order_row(&row)
    }

    pub async fn expire_unfunded_order(
        &self,
        id: Uuid,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<Option<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
                UPDATE router_orders ro
                SET
                    status = 'expired',
                    updated_at = $2
                WHERE ro.id = $1
                  AND ro.status IN ('quoted', 'pending_funding')
                  AND ro.action_timeout_at <= $2
                  AND (
                      ro.funding_vault_id IS NULL
                      OR EXISTS (
                          SELECT 1
                          FROM deposit_vaults funding_vault
                          WHERE funding_vault.id = ro.funding_vault_id
                            AND funding_vault.status = 'pending_funding'
                            AND funding_vault.funding_observed_amount IS NULL
                      )
                  )
                RETURNING ro.*
            )
            SELECT {ORDER_SELECT_COLUMNS}
            FROM updated ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            "#
        ))
        .bind(id)
        .bind(now)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.expire_unfunded_order",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        row.map(|row| self.map_order_row(&row)).transpose()
    }

    pub async fn expire_unfunded_orders(
        &self,
        now: DateTime<Utc>,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH candidates AS (
                SELECT ro.id
                FROM router_orders ro
                WHERE ro.status IN ('quoted', 'pending_funding')
                  AND ro.action_timeout_at <= $1
                  AND (
                      ro.funding_vault_id IS NULL
                      OR EXISTS (
                          SELECT 1
                          FROM deposit_vaults funding_vault
                          WHERE funding_vault.id = ro.funding_vault_id
                            AND funding_vault.status = 'pending_funding'
                            AND funding_vault.funding_observed_amount IS NULL
                      )
                  )
                ORDER BY ro.action_timeout_at ASC, ro.id ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE router_orders ro
                SET
                    status = 'expired',
                    updated_at = $1
                FROM candidates
                WHERE ro.id = candidates.id
                RETURNING ro.*
            )
            SELECT {ORDER_SELECT_COLUMNS}
            FROM updated ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            "#
        ))
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.expire_unfunded_orders",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn find_unstarted_orders_with_refunded_funding_vault(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN deposit_vaults dv ON dv.id = ro.funding_vault_id
            WHERE (
                ro.status IN (
                    'pending_funding',
                    'funded',
                    'refund_required',
                    'refunding'
                )
                OR (
                    ro.status = 'expired'
                    AND dv.funding_observed_at IS NOT NULL
                    AND dv.funding_observed_at <= ro.action_timeout_at
                )
              )
              AND dv.status = 'refunded'
              AND NOT EXISTS (
                  SELECT 1
                  FROM order_execution_steps oes
                  WHERE oes.order_id = ro.id
                    AND oes.step_index > 0
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_unstarted_orders_with_refunded_funding_vault",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn has_execution_steps_after_deposit(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<bool> {
        let started = Instant::now();
        let result = sqlx_core::query_scalar::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM order_execution_steps
                WHERE order_id = $1
                  AND step_index > 0
            )
            "#,
        )
        .bind(order_id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.has_execution_steps_after_deposit",
            result.is_ok(),
            started.elapsed(),
        );
        Ok(result?)
    }

    pub async fn get_market_orders_needing_execution_plan(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN deposit_vaults dv ON dv.id = ro.funding_vault_id
            WHERE ro.order_type IN ('market_order', 'limit_order')
              AND ro.status IN ('pending_funding', 'funded')
              AND dv.status = 'funded'
              AND NOT EXISTS (
                  SELECT 1
                  FROM order_execution_steps oes
                  WHERE oes.order_id = ro.id
                    AND oes.step_index > 0
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_market_orders_needing_execution_plan",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn find_orders_with_failed_steps(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<(Uuid, Option<Uuid>)>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            SELECT DISTINCT ro.id, ro.funding_vault_id
            FROM router_orders ro
            JOIN order_execution_steps oes ON oes.order_id = ro.id
            WHERE ro.status IN ('funded', 'executing')
              AND oes.status = 'failed'
            ORDER BY ro.id ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_orders_with_failed_steps",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| Ok((row.try_get("id")?, row.try_get("funding_vault_id")?)))
            .collect()
    }

    pub async fn get_market_orders_ready_for_execution(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT DISTINCT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN order_execution_attempts oea
              ON oea.order_id = ro.id
             AND oea.status = 'active'
            JOIN order_execution_steps oes
              ON oes.execution_attempt_id = oea.id
            WHERE ro.order_type IN ('market_order', 'limit_order')
              AND ro.status IN ('funded', 'executing', 'refund_required', 'refunding')
              AND oes.step_index > 0
              AND oes.status IN ('planned', 'ready')
              AND (oes.next_attempt_at IS NULL OR oes.next_attempt_at <= NOW())
              AND NOT EXISTS (
                  SELECT 1
                  FROM order_execution_steps prior
                  WHERE prior.execution_attempt_id = oea.id
                    AND prior.step_index > 0
                    AND prior.step_index < oes.step_index
                    AND prior.status <> 'completed'
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_market_orders_ready_for_execution",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn find_executing_orders_pending_completion_finalization(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT DISTINCT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN order_execution_attempts oea
              ON oea.order_id = ro.id
             AND oea.status = 'active'
            WHERE ro.order_type IN ('market_order', 'limit_order')
              AND ro.status IN ('executing', 'refunding')
              AND EXISTS (
                  SELECT 1
                  FROM order_execution_steps oes
                  WHERE oes.execution_attempt_id = oea.id
                    AND oes.step_index > 0
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM order_execution_steps oes
                  WHERE oes.execution_attempt_id = oea.id
                    AND oes.step_index > 0
                    AND oes.status <> 'completed'
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_executing_orders_pending_completion_finalization",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn get_market_order_quote(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<MarketOrderQuote> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            "SELECT {QUOTE_SELECT_COLUMNS} FROM market_order_quotes WHERE order_id = $1"
        ))
        .bind(order_id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_market_order_quote",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_quote_row(&row)
    }

    pub async fn get_market_order_quote_by_id(
        &self,
        quote_id: Uuid,
    ) -> RouterCoreResult<MarketOrderQuote> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            "SELECT {QUOTE_SELECT_COLUMNS} FROM market_order_quotes WHERE id = $1"
        ))
        .bind(quote_id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_market_order_quote_by_id",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_quote_row(&row)
    }

    pub async fn get_limit_order_quote(&self, order_id: Uuid) -> RouterCoreResult<LimitOrderQuote> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            "SELECT {LIMIT_QUOTE_SELECT_COLUMNS} FROM limit_order_quotes WHERE order_id = $1"
        ))
        .bind(order_id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_limit_order_quote",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_limit_quote_row(&row)
    }

    pub async fn get_limit_order_quote_by_id(
        &self,
        quote_id: Uuid,
    ) -> RouterCoreResult<LimitOrderQuote> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            "SELECT {LIMIT_QUOTE_SELECT_COLUMNS} FROM limit_order_quotes WHERE id = $1"
        ))
        .bind(quote_id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_limit_order_quote_by_id",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_limit_quote_row(&row)
    }

    pub async fn get_router_order_quote_by_id(
        &self,
        quote_id: Uuid,
    ) -> RouterCoreResult<RouterOrderQuote> {
        match self.get_market_order_quote_by_id(quote_id).await {
            Ok(quote) => Ok(quote.into()),
            Err(RouterCoreError::NotFound) => self
                .get_limit_order_quote_by_id(quote_id)
                .await
                .map(Into::into),
            Err(err) => Err(err),
        }
    }

    pub async fn get_router_order_quote(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<RouterOrderQuote> {
        match self.get_market_order_quote(order_id).await {
            Ok(quote) => Ok(quote.into()),
            Err(RouterCoreError::NotFound) => {
                self.get_limit_order_quote(order_id).await.map(Into::into)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn delete_expired_unassociated_market_order_quotes(
        &self,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<u64> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            DELETE FROM market_order_quotes
            WHERE order_id IS NULL
              AND expires_at <= $1
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.delete_expired_unassociated_market_order_quotes",
            result.is_ok(),
            started.elapsed(),
        );
        Ok(result?.rows_affected())
    }

    pub async fn delete_expired_unassociated_router_order_quotes(
        &self,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<u64> {
        let deleted_market = self
            .delete_expired_unassociated_market_order_quotes(now)
            .await?;
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            DELETE FROM limit_order_quotes
            WHERE order_id IS NULL
              AND expires_at <= $1
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.delete_expired_unassociated_limit_order_quotes",
            result.is_ok(),
            started.elapsed(),
        );
        Ok(deleted_market + result?.rows_affected())
    }

    pub async fn create_execution_attempt(
        &self,
        attempt: &OrderExecutionAttempt,
    ) -> RouterCoreResult<()> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            INSERT INTO order_execution_attempts (
                id,
                order_id,
                attempt_index,
                attempt_kind,
                status,
                trigger_step_id,
                trigger_provider_operation_id,
                failure_reason_json,
                input_custody_snapshot_json,
                superseded_by_attempt_id,
                superseded_reason_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(attempt.id)
        .bind(attempt.order_id)
        .bind(attempt.attempt_index)
        .bind(attempt.attempt_kind.to_db_string())
        .bind(attempt.status.to_db_string())
        .bind(attempt.trigger_step_id)
        .bind(attempt.trigger_provider_operation_id)
        .bind(attempt.failure_reason.clone())
        .bind(attempt.input_custody_snapshot.clone())
        .bind(Option::<Uuid>::None)
        .bind(serde_json::json!({}))
        .bind(attempt.created_at)
        .bind(attempt.updated_at)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_execution_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    pub async fn materialize_primary_execution_attempt(
        &self,
        order_id: Uuid,
        plan: ExecutionAttemptPlan,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<ExecutionAttemptMaterializationRecord> {
        let started = Instant::now();
        let result = async {
            if plan.legs.is_empty() || plan.steps.is_empty() {
                return Err(RouterCoreError::Validation {
                    message: format!(
                        "order {order_id} execution attempt requires at least one leg and step"
                    ),
                });
            }

            let mut tx = self.pool.begin().await?;

            let order_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                FOR UPDATE OF ro
                "#
            ))
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;
            match order.status {
                RouterOrderStatus::Funded | RouterOrderStatus::Executing => {}
                status => {
                    return Err(RouterCoreError::Conflict {
                        message: format!(
                            "order {} cannot start execution from {}",
                            order.id,
                            status.to_db_string()
                        ),
                    });
                }
            }

            if let Some(funding_vault_id) = order.funding_vault_id {
                let updated = sqlx_core::query::query(
                    r#"
                    UPDATE deposit_vaults
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = $4
                    "#,
                )
                .bind(funding_vault_id)
                .bind(DepositVaultStatus::Executing.to_db_string())
                .bind(now)
                .bind(DepositVaultStatus::Funded.to_db_string())
                .execute(&mut *tx)
                .await?;
                if updated.rows_affected() == 0 {
                    let vault_status = sqlx_core::query_scalar::query_scalar::<_, String>(
                        "SELECT status FROM deposit_vaults WHERE id = $1",
                    )
                    .bind(funding_vault_id)
                    .fetch_one(&mut *tx)
                    .await?;
                    let Some(vault_status) = DepositVaultStatus::from_db_string(&vault_status)
                    else {
                        return Err(RouterCoreError::InvalidData {
                            message: format!(
                                "unknown deposit vault status for vault {funding_vault_id}"
                            ),
                        });
                    };
                    if !matches!(
                        vault_status,
                        DepositVaultStatus::Executing | DepositVaultStatus::Completed
                    ) {
                        return Err(RouterCoreError::Conflict {
                            message: format!(
                                "funding vault {} cannot start execution from {}",
                                funding_vault_id,
                                vault_status.to_db_string()
                            ),
                        });
                    }
                }
            }

            let attempt_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE order_id = $1
                  AND attempt_kind = $2
                  AND attempt_index = 1
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                FOR UPDATE
                "#
            ))
            .bind(order_id)
            .bind(OrderExecutionAttemptKind::PrimaryExecution.to_db_string())
            .fetch_optional(&mut *tx)
            .await?;
            let attempt = if let Some(row) = attempt_row {
                let attempt = self.map_execution_attempt_row(&row)?;
                if !matches!(
                    attempt.status,
                    OrderExecutionAttemptStatus::Planning
                        | OrderExecutionAttemptStatus::Active
                        | OrderExecutionAttemptStatus::Completed
                ) {
                    return Err(RouterCoreError::Conflict {
                        message: format!(
                            "order {} primary execution attempt {} is {}",
                            order_id,
                            attempt.id,
                            attempt.status.to_db_string()
                        ),
                    });
                }
                if attempt.status == OrderExecutionAttemptStatus::Planning {
                    let row = sqlx_core::query::query(&format!(
                        r#"
                        UPDATE order_execution_attempts
                        SET status = $2, updated_at = $3
                        WHERE id = $1
                          AND status = $4
                        RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                        "#
                    ))
                    .bind(attempt.id)
                    .bind(OrderExecutionAttemptStatus::Active.to_db_string())
                    .bind(now)
                    .bind(OrderExecutionAttemptStatus::Planning.to_db_string())
                    .fetch_one(&mut *tx)
                    .await?;
                    self.map_execution_attempt_row(&row)?
                } else {
                    attempt
                }
            } else {
                let attempt = OrderExecutionAttempt {
                    id: Uuid::now_v7(),
                    order_id,
                    attempt_index: 1,
                    attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
                    status: OrderExecutionAttemptStatus::Active,
                    trigger_step_id: None,
                    trigger_provider_operation_id: None,
                    failure_reason: serde_json::json!({}),
                    input_custody_snapshot: serde_json::json!({}),
                    created_at: now,
                    updated_at: now,
                };
                let row = sqlx_core::query::query(&format!(
                    r#"
                    INSERT INTO order_execution_attempts (
                        id,
                        order_id,
                        attempt_index,
                        attempt_kind,
                        status,
                        trigger_step_id,
                        trigger_provider_operation_id,
                        failure_reason_json,
                        input_custody_snapshot_json,
                        superseded_by_attempt_id,
                        superseded_reason_json,
                        created_at,
                        updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, '{{}}'::jsonb, $10, $11)
                    RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                    "#
                ))
                .bind(attempt.id)
                .bind(attempt.order_id)
                .bind(attempt.attempt_index)
                .bind(attempt.attempt_kind.to_db_string())
                .bind(attempt.status.to_db_string())
                .bind(attempt.trigger_step_id)
                .bind(attempt.trigger_provider_operation_id)
                .bind(attempt.failure_reason)
                .bind(attempt.input_custody_snapshot)
                .bind(attempt.created_at)
                .bind(attempt.updated_at)
                .fetch_one(&mut *tx)
                .await?;
                self.map_execution_attempt_row(&row)?
            };

            let mut leg_id_by_planned_id = HashMap::new();
            let mut materialized_legs = Vec::with_capacity(plan.legs.len());
            for mut leg in plan.legs {
                let planned_leg_id = leg.id;
                leg.order_id = order_id;
                leg.execution_attempt_id = Some(attempt.id);
                leg.status = OrderExecutionStepStatus::Planned;
                leg.started_at = None;
                leg.completed_at = None;
                leg.updated_at = now;
                let leg_row = sqlx_core::query::query(&format!(
                    r#"
                    WITH inserted AS (
                        INSERT INTO order_execution_legs (
                            id,
                            order_id,
                            execution_attempt_id,
                            transition_decl_id,
                            leg_index,
                            leg_type,
                            provider,
                            status,
                            input_chain_id,
                            input_asset_id,
                            output_chain_id,
                            output_asset_id,
                            amount_in,
                            expected_amount_out,
                            min_amount_out,
                            actual_amount_in,
                            actual_amount_out,
                            started_at,
                            completed_at,
                            provider_quote_expires_at,
                            details_json,
                            usd_valuation_json,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                            $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
                        )
                        ON CONFLICT (execution_attempt_id, leg_index)
                        WHERE execution_attempt_id IS NOT NULL DO NOTHING
                        RETURNING {EXECUTION_LEG_SELECT_COLUMNS}
                    )
                    SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                    FROM inserted
                    UNION ALL
                    SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                    FROM order_execution_legs
                    WHERE execution_attempt_id = $3
                      AND leg_index = $5
                    LIMIT 1
                    "#
                ))
                .bind(leg.id)
                .bind(leg.order_id)
                .bind(leg.execution_attempt_id)
                .bind(leg.transition_decl_id.clone())
                .bind(leg.leg_index)
                .bind(&leg.leg_type)
                .bind(&leg.provider)
                .bind(leg.status.to_db_string())
                .bind(leg.input_asset.chain.as_str())
                .bind(leg.input_asset.asset.as_str())
                .bind(leg.output_asset.chain.as_str())
                .bind(leg.output_asset.asset.as_str())
                .bind(&leg.amount_in)
                .bind(&leg.expected_amount_out)
                .bind(leg.min_amount_out.clone())
                .bind(leg.actual_amount_in.clone())
                .bind(leg.actual_amount_out.clone())
                .bind(leg.started_at)
                .bind(leg.completed_at)
                .bind(leg.provider_quote_expires_at)
                .bind(leg.details.clone())
                .bind(leg.usd_valuation.clone())
                .bind(leg.created_at)
                .bind(leg.updated_at)
                .fetch_one(&mut *tx)
                .await?;
                let materialized_leg = self.map_execution_leg_row(&leg_row)?;
                ensure_execution_leg_plan_matches(&materialized_leg, &leg)?;
                leg_id_by_planned_id.insert(planned_leg_id, materialized_leg.id);
                materialized_legs.push(materialized_leg);
            }

            let single_leg_id = if materialized_legs.len() == 1 {
                Some(materialized_legs[0].id)
            } else {
                None
            };
            let mut materialized_steps = Vec::with_capacity(plan.steps.len());
            for mut step in plan.steps {
                let materialized_leg_id = match step.execution_leg_id {
                    Some(planned_leg_id) => leg_id_by_planned_id
                        .get(&planned_leg_id)
                        .copied()
                        .ok_or_else(|| RouterCoreError::InvalidData {
                            message: format!(
                                "order {order_id} step {} references unknown execution leg {}",
                                step.step_index, planned_leg_id
                            ),
                        })?,
                    None => single_leg_id.ok_or_else(|| RouterCoreError::InvalidData {
                        message: format!(
                            "order {order_id} step {} has no execution_leg_id in a multi-leg plan",
                            step.step_index
                        ),
                    })?,
                };
                step.order_id = order_id;
                step.execution_attempt_id = Some(attempt.id);
                step.execution_leg_id = Some(materialized_leg_id);
                step.status = OrderExecutionStepStatus::Planned;
                step.idempotency_key = Some(format!("attempt:{}:step:{}", attempt.id, step.id));
                step.attempt_count = 0;
                step.started_at = None;
                step.completed_at = None;
                step.updated_at = now;
                let step_row = sqlx_core::query::query(&format!(
                    r#"
                    WITH inserted AS (
                        INSERT INTO order_execution_steps (
                            id,
                            order_id,
                            execution_attempt_id,
                            execution_leg_id,
                            transition_decl_id,
                            step_index,
                            step_type,
                            provider,
                            status,
                            input_chain_id,
                            input_asset_id,
                            output_chain_id,
                            output_asset_id,
                            amount_in,
                            min_amount_out,
                            tx_hash,
                            provider_ref,
                            idempotency_key,
                            attempt_count,
                            next_attempt_at,
                            started_at,
                            completed_at,
                            details_json,
                            request_json,
                            response_json,
                            error_json,
                            usd_valuation_json,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                            $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
                            $23, $24, $25, $26, $27, $28, $29
                        )
                        ON CONFLICT (execution_attempt_id, step_index)
                        WHERE execution_attempt_id IS NOT NULL DO NOTHING
                        RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
                    )
                    SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                    FROM inserted
                    UNION ALL
                    SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                    FROM order_execution_steps
                    WHERE execution_attempt_id = $3
                      AND step_index = $6
                    LIMIT 1
                    "#
                ))
                .bind(step.id)
                .bind(step.order_id)
                .bind(step.execution_attempt_id)
                .bind(step.execution_leg_id)
                .bind(step.transition_decl_id.clone())
                .bind(step.step_index)
                .bind(step.step_type.to_db_string())
                .bind(&step.provider)
                .bind(step.status.to_db_string())
                .bind(step.input_asset.as_ref().map(|asset| asset.chain.as_str()))
                .bind(step.input_asset.as_ref().map(|asset| asset.asset.as_str()))
                .bind(step.output_asset.as_ref().map(|asset| asset.chain.as_str()))
                .bind(step.output_asset.as_ref().map(|asset| asset.asset.as_str()))
                .bind(step.amount_in.clone())
                .bind(step.min_amount_out.clone())
                .bind(step.tx_hash.clone())
                .bind(step.provider_ref.clone())
                .bind(step.idempotency_key.clone())
                .bind(step.attempt_count)
                .bind(step.next_attempt_at)
                .bind(step.started_at)
                .bind(step.completed_at)
                .bind(step.details.clone())
                .bind(step.request.clone())
                .bind(step.response.clone())
                .bind(step.error.clone())
                .bind(step.usd_valuation.clone())
                .bind(step.created_at)
                .bind(step.updated_at)
                .fetch_one(&mut *tx)
                .await?;
                let materialized_step = self.map_execution_step_row(&step_row)?;
                ensure_execution_step_plan_matches(&materialized_step, &step)?;
                materialized_steps.push(materialized_step);
            }
            materialized_legs.sort_by_key(|leg| (leg.leg_index, leg.created_at, leg.id));
            materialized_steps.sort_by_key(|step| (step.step_index, step.created_at, step.id));

            let order_row = sqlx_core::query::query(&format!(
                r#"
                WITH updated AS (
                    UPDATE router_orders
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = $4
                    RETURNING *
                )
                SELECT {ORDER_SELECT_COLUMNS}
                FROM updated ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                UNION ALL
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                  AND ro.status = $2
                LIMIT 1
                "#
            ))
            .bind(order_id)
            .bind(RouterOrderStatus::Executing.to_db_string())
            .bind(now)
            .bind(RouterOrderStatus::Funded.to_db_string())
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;

            tx.commit().await?;
            Ok::<ExecutionAttemptMaterializationRecord, RouterCoreError>(
                ExecutionAttemptMaterializationRecord {
                    order,
                    attempt,
                    legs: materialized_legs,
                    steps: materialized_steps,
                },
            )
        }
        .await;
        telemetry::record_db_query(
            "order.materialize_primary_execution_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn persist_execution_step_ready_to_fire(
        &self,
        order_id: Uuid,
        attempt_id: Uuid,
        step_id: Uuid,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
                UPDATE order_execution_steps
                SET
                    status = 'running',
                    attempt_count = attempt_count + 1,
                    started_at = COALESCE(started_at, $4),
                    updated_at = $4
                WHERE id = $1
                  AND order_id = $2
                  AND execution_attempt_id = $3
                  AND status IN ('planned', 'ready')
                RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            )
            SELECT {EXECUTION_STEP_SELECT_COLUMNS}
            FROM updated
            UNION ALL
            SELECT {EXECUTION_STEP_SELECT_COLUMNS}
            FROM order_execution_steps
            WHERE id = $1
              AND order_id = $2
              AND execution_attempt_id = $3
              AND status IN ('running', 'waiting', 'completed')
              AND NOT EXISTS (SELECT 1 FROM updated)
            LIMIT 1
            "#
        ))
        .bind(step_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(now)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.persist_execution_step_ready_to_fire",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;
        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn persist_execution_step_failed(
        &self,
        order_id: Uuid,
        attempt_id: Uuid,
        step_id: Uuid,
        error: serde_json::Value,
        failed_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH failed AS (
                UPDATE order_execution_steps
                SET
                    status = 'failed',
                    error_json = $4,
                    completed_at = COALESCE(completed_at, $5),
                    updated_at = $5
                WHERE id = $1
                  AND order_id = $2
                  AND execution_attempt_id = $3
                  AND status = 'running'
                RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            )
            SELECT {EXECUTION_STEP_SELECT_COLUMNS}
            FROM failed
            UNION ALL
            SELECT {EXECUTION_STEP_SELECT_COLUMNS}
            FROM order_execution_steps
            WHERE id = $1
              AND order_id = $2
              AND execution_attempt_id = $3
              AND status IN ('failed', 'superseded')
              AND NOT EXISTS (SELECT 1 FROM failed)
            LIMIT 1
            "#
        ))
        .bind(step_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(error)
        .bind(failed_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.persist_execution_step_failed",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;
        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn create_retry_execution_attempt_from_failed_step(
        &self,
        order_id: Uuid,
        failed_attempt_id: Uuid,
        failed_step_id: Uuid,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<ExecutionAttemptMaterializationRecord> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;

            let order_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                FOR UPDATE OF ro
                "#
            ))
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;

            let failed_attempt_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE id = $1
                FOR UPDATE
                "#
            ))
            .bind(failed_attempt_id)
            .fetch_one(&mut *tx)
            .await?;
            let failed_attempt = self.map_execution_attempt_row(&failed_attempt_row)?;
            if failed_attempt.order_id != order_id {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "attempt {} does not belong to order {}",
                        failed_attempt.id, order_id
                    ),
                });
            }
            if failed_attempt.status != OrderExecutionAttemptStatus::Failed {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "attempt {} cannot be retried from {}",
                        failed_attempt.id,
                        failed_attempt.status.to_db_string()
                    ),
                });
            }

            let failed_step_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                FROM order_execution_steps
                WHERE id = $1
                FOR UPDATE
                "#
            ))
            .bind(failed_step_id)
            .fetch_one(&mut *tx)
            .await?;
            let failed_step = self.map_execution_step_row(&failed_step_row)?;
            if failed_step.order_id != order_id
                || failed_step.execution_attempt_id != Some(failed_attempt.id)
            {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "step {} does not belong to order {} attempt {}",
                        failed_step.id, order_id, failed_attempt.id
                    ),
                });
            }

            let retry_attempt_index = failed_attempt.attempt_index + 1;
            if let Some(existing_retry_row) = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE order_id = $1
                  AND attempt_index = $2
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                FOR UPDATE
                "#
            ))
            .bind(order_id)
            .bind(retry_attempt_index)
            .fetch_optional(&mut *tx)
            .await?
            {
                let attempt = self.map_execution_attempt_row(&existing_retry_row)?;
                let leg_rows = sqlx_core::query::query(&format!(
                    r#"
                    SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                    FROM order_execution_legs
                    WHERE execution_attempt_id = $1
                    ORDER BY leg_index ASC, created_at ASC, id ASC
                    "#
                ))
                .bind(attempt.id)
                .fetch_all(&mut *tx)
                .await?;
                let legs = leg_rows
                    .iter()
                    .map(|row| self.map_execution_leg_row(row))
                    .collect::<RouterCoreResult<Vec<_>>>()?;
                let step_rows = sqlx_core::query::query(&format!(
                    r#"
                    SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                    FROM order_execution_steps
                    WHERE execution_attempt_id = $1
                    ORDER BY step_index ASC, created_at ASC, id ASC
                    "#
                ))
                .bind(attempt.id)
                .fetch_all(&mut *tx)
                .await?;
                let steps = step_rows
                    .iter()
                    .map(|row| self.map_execution_step_row(row))
                    .collect::<RouterCoreResult<Vec<_>>>()?;
                tx.commit().await?;
                return Ok::<ExecutionAttemptMaterializationRecord, RouterCoreError>(
                    ExecutionAttemptMaterializationRecord {
                        order,
                        attempt,
                        legs,
                        steps,
                    },
                );
            }

            let retry_attempt = OrderExecutionAttempt {
                id: Uuid::now_v7(),
                order_id,
                attempt_index: retry_attempt_index,
                attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
                status: OrderExecutionAttemptStatus::Active,
                trigger_step_id: Some(failed_step.id),
                trigger_provider_operation_id: failed_attempt.trigger_provider_operation_id,
                failure_reason: serde_json::json!({
                    "reason": "retry_after_failed_attempt",
                    "failed_attempt_id": failed_attempt.id,
                    "failed_attempt_index": failed_attempt.attempt_index,
                    "failed_step_id": failed_step.id,
                    "failed_step_type": failed_step.step_type.to_db_string(),
                    "failed_step_error": &failed_step.error,
                }),
                input_custody_snapshot: failed_attempt.input_custody_snapshot.clone(),
                created_at: now,
                updated_at: now,
            };
            let retry_attempt_row = sqlx_core::query::query(&format!(
                r#"
                INSERT INTO order_execution_attempts (
                    id,
                    order_id,
                    attempt_index,
                    attempt_kind,
                    status,
                    trigger_step_id,
                    trigger_provider_operation_id,
                    failure_reason_json,
                    input_custody_snapshot_json,
                    superseded_by_attempt_id,
                    superseded_reason_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, '{{}}'::jsonb, $10, $11)
                RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                "#
            ))
            .bind(retry_attempt.id)
            .bind(retry_attempt.order_id)
            .bind(retry_attempt.attempt_index)
            .bind(retry_attempt.attempt_kind.to_db_string())
            .bind(retry_attempt.status.to_db_string())
            .bind(retry_attempt.trigger_step_id)
            .bind(retry_attempt.trigger_provider_operation_id)
            .bind(retry_attempt.failure_reason.clone())
            .bind(retry_attempt.input_custody_snapshot.clone())
            .bind(retry_attempt.created_at)
            .bind(retry_attempt.updated_at)
            .fetch_one(&mut *tx)
            .await?;
            let retry_attempt = self.map_execution_attempt_row(&retry_attempt_row)?;

            let retry_step_rows = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                FROM order_execution_steps
                WHERE execution_attempt_id = $1
                  AND step_index >= $2
                  AND status IN ('failed', 'planned', 'ready', 'waiting', 'running')
                ORDER BY step_index ASC, created_at ASC, id ASC
                "#
            ))
            .bind(failed_attempt.id)
            .bind(failed_step.step_index)
            .fetch_all(&mut *tx)
            .await?;
            let retry_source_steps = retry_step_rows
                .iter()
                .map(|row| self.map_execution_step_row(row))
                .collect::<RouterCoreResult<Vec<_>>>()?;
            if retry_source_steps.is_empty() {
                return Err(RouterCoreError::InvalidData {
                    message: format!(
                        "attempt {} has no retryable steps from step_index {}",
                        failed_attempt.id, failed_step.step_index
                    ),
                });
            }

            let referenced_leg_ids = retry_source_steps
                .iter()
                .filter_map(|step| step.execution_leg_id)
                .collect::<std::collections::BTreeSet<_>>();
            let failed_attempt_legs = if referenced_leg_ids.is_empty() {
                Vec::new()
            } else {
                let leg_ids = referenced_leg_ids.iter().copied().collect::<Vec<_>>();
                let rows = sqlx_core::query::query(&format!(
                    r#"
                    SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                    FROM order_execution_legs
                    WHERE id = ANY($1)
                    ORDER BY leg_index ASC, created_at ASC, id ASC
                    "#
                ))
                .bind(leg_ids)
                .fetch_all(&mut *tx)
                .await?;
                rows.iter()
                    .map(|row| self.map_execution_leg_row(row))
                    .collect::<RouterCoreResult<Vec<_>>>()?
            };

            let mut retry_leg_ids = HashMap::new();
            let mut retry_legs = Vec::with_capacity(failed_attempt_legs.len());
            for failed_leg in failed_attempt_legs {
                let failed_leg_id = failed_leg.id;
                let mut retry_leg = failed_leg;
                retry_leg.id = Uuid::now_v7();
                retry_leg.execution_attempt_id = Some(retry_attempt.id);
                retry_leg.status = OrderExecutionStepStatus::Planned;
                retry_leg.actual_amount_in = None;
                retry_leg.actual_amount_out = None;
                retry_leg.started_at = None;
                retry_leg.completed_at = None;
                retry_leg.created_at = now;
                retry_leg.updated_at = now;
                set_json_value(
                    &mut retry_leg.details,
                    "retry_attempt_from_attempt_id",
                    serde_json::json!(failed_attempt.id),
                );
                set_json_value(
                    &mut retry_leg.details,
                    "retry_attempt_from_attempt_index",
                    serde_json::json!(failed_attempt.attempt_index),
                );
                set_json_value(
                    &mut retry_leg.details,
                    "retry_attempt_trigger_step_id",
                    serde_json::json!(failed_step.id),
                );
                set_json_value(
                    &mut retry_leg.details,
                    "retry_attempt_from_execution_leg_id",
                    serde_json::json!(failed_leg_id),
                );
                retry_leg_ids.insert(failed_leg_id, retry_leg.id);
                retry_legs.push(retry_leg);
            }
            if retry_leg_ids.len() != referenced_leg_ids.len() {
                let missing = referenced_leg_ids
                    .iter()
                    .filter(|leg_id| !retry_leg_ids.contains_key(leg_id))
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(RouterCoreError::InvalidData {
                    message: format!(
                        "retry attempt {} could not clone execution legs from failed attempt {}: missing leg ids [{}]",
                        retry_attempt.id, failed_attempt.id, missing
                    ),
                });
            }

            for leg in &retry_legs {
                insert_execution_leg_tx(&mut tx, leg).await?;
            }

            let mut retry_steps = Vec::with_capacity(retry_source_steps.len());
            for mut step in retry_source_steps {
                step.id = Uuid::now_v7();
                step.execution_attempt_id = Some(retry_attempt.id);
                if let Some(execution_leg_id) = step.execution_leg_id {
                    step.execution_leg_id = retry_leg_ids.get(&execution_leg_id).copied();
                }
                step.status = OrderExecutionStepStatus::Planned;
                step.tx_hash = None;
                step.provider_ref = None;
                step.idempotency_key = Some(format!(
                    "order:{order_id}:attempt:{retry_attempt_index}:{}:{}",
                    step.provider, step.step_index
                ));
                step.attempt_count = 0;
                step.next_attempt_at = None;
                step.started_at = None;
                step.completed_at = None;
                step.response = serde_json::json!({});
                step.error = serde_json::json!({});
                step.created_at = now;
                step.updated_at = now;
                set_json_value(
                    &mut step.details,
                    "retry_attempt_from_attempt_id",
                    serde_json::json!(failed_attempt.id),
                );
                set_json_value(
                    &mut step.details,
                    "retry_attempt_from_attempt_index",
                    serde_json::json!(failed_attempt.attempt_index),
                );
                set_json_value(
                    &mut step.details,
                    "retry_attempt_trigger_step_id",
                    serde_json::json!(failed_step.id),
                );
                insert_execution_step_tx(&mut tx, &step).await?;
                retry_steps.push(step);
            }

            let superseded_reason = serde_json::json!({
                "reason": "superseded_by_retry_attempt",
                "retry_attempt_id": retry_attempt.id,
                "retry_attempt_index": retry_attempt.attempt_index,
            });
            sqlx_core::query::query(
                r#"
                UPDATE order_execution_steps
                SET
                    status = 'superseded',
                    error_json = $3,
                    completed_at = COALESCE(completed_at, $4),
                    updated_at = $4
                WHERE execution_attempt_id = $1
                  AND step_index >= $2
                  AND status IN ('failed', 'planned', 'ready', 'waiting', 'running')
                "#,
            )
            .bind(failed_attempt.id)
            .bind(failed_step.step_index)
            .bind(superseded_reason.clone())
            .bind(now)
            .execute(&mut *tx)
            .await?;
            sqlx_core::query::query(
                r#"
                UPDATE order_execution_attempts
                SET
                    superseded_by_attempt_id = $2,
                    superseded_reason_json = $3,
                    updated_at = $4
                WHERE id = $1
                "#,
            )
            .bind(failed_attempt.id)
            .bind(retry_attempt.id)
            .bind(superseded_reason)
            .bind(now)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok::<ExecutionAttemptMaterializationRecord, RouterCoreError>(
                ExecutionAttemptMaterializationRecord {
                    order,
                    attempt: retry_attempt,
                    legs: retry_legs,
                    steps: retry_steps,
                },
            )
        }
        .await;
        telemetry::record_db_query(
            "order.create_retry_execution_attempt_from_failed_step",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn create_refreshed_execution_attempt_from_failed_step(
        &self,
        order_id: Uuid,
        stale_attempt_id: Uuid,
        failed_step_id: Uuid,
        plan: RefreshedExecutionAttemptPlan,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<ExecutionAttemptMaterializationRecord> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;

            let order_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                FOR UPDATE OF ro
                "#
            ))
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;

            let stale_attempt_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE id = $1
                FOR UPDATE
                "#
            ))
            .bind(stale_attempt_id)
            .fetch_one(&mut *tx)
            .await?;
            let stale_attempt = self.map_execution_attempt_row(&stale_attempt_row)?;
            if stale_attempt.order_id != order_id {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "attempt {} does not belong to order {}",
                        stale_attempt.id, order_id
                    ),
                });
            }
            if stale_attempt.status != OrderExecutionAttemptStatus::Failed {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "attempt {} cannot be refreshed from {}",
                        stale_attempt.id,
                        stale_attempt.status.to_db_string()
                    ),
                });
            }

            let failed_step_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                FROM order_execution_steps
                WHERE id = $1
                FOR UPDATE
                "#
            ))
            .bind(failed_step_id)
            .fetch_one(&mut *tx)
            .await?;
            let failed_step = self.map_execution_step_row(&failed_step_row)?;
            if failed_step.order_id != order_id
                || failed_step.execution_attempt_id != Some(stale_attempt.id)
            {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "step {} does not belong to order {} attempt {}",
                        failed_step.id, order_id, stale_attempt.id
                    ),
                });
            }

            let refreshed_attempt_index = stale_attempt.attempt_index + 1;
            if let Some(existing_refreshed_row) = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE order_id = $1
                  AND attempt_index = $2
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                FOR UPDATE
                "#
            ))
            .bind(order_id)
            .bind(refreshed_attempt_index)
            .fetch_optional(&mut *tx)
            .await?
            {
                let attempt = self.map_execution_attempt_row(&existing_refreshed_row)?;
                if attempt.attempt_kind != OrderExecutionAttemptKind::RefreshedExecution {
                    return Err(RouterCoreError::Conflict {
                        message: format!(
                            "order {} attempt_index {} already materialized as {}",
                            order_id,
                            refreshed_attempt_index,
                            attempt.attempt_kind.to_db_string()
                        ),
                    });
                }
                let leg_rows = sqlx_core::query::query(&format!(
                    r#"
                    SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                    FROM order_execution_legs
                    WHERE execution_attempt_id = $1
                    ORDER BY leg_index ASC, created_at ASC, id ASC
                    "#
                ))
                .bind(attempt.id)
                .fetch_all(&mut *tx)
                .await?;
                let legs = leg_rows
                    .iter()
                    .map(|row| self.map_execution_leg_row(row))
                    .collect::<RouterCoreResult<Vec<_>>>()?;
                let step_rows = sqlx_core::query::query(&format!(
                    r#"
                    SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                    FROM order_execution_steps
                    WHERE execution_attempt_id = $1
                    ORDER BY step_index ASC, created_at ASC, id ASC
                    "#
                ))
                .bind(attempt.id)
                .fetch_all(&mut *tx)
                .await?;
                let steps = step_rows
                    .iter()
                    .map(|row| self.map_execution_step_row(row))
                    .collect::<RouterCoreResult<Vec<_>>>()?;
                tx.commit().await?;
                return Ok::<ExecutionAttemptMaterializationRecord, RouterCoreError>(
                    ExecutionAttemptMaterializationRecord {
                        order,
                        attempt,
                        legs,
                        steps,
                    },
                );
            }

            if plan.steps.is_empty() {
                return Err(RouterCoreError::InvalidData {
                    message: format!(
                        "refreshed attempt for order {} has no execution steps",
                        order_id
                    ),
                });
            }

            let refreshed_attempt = OrderExecutionAttempt {
                id: Uuid::now_v7(),
                order_id,
                attempt_index: refreshed_attempt_index,
                attempt_kind: OrderExecutionAttemptKind::RefreshedExecution,
                status: OrderExecutionAttemptStatus::Active,
                trigger_step_id: Some(failed_step.id),
                trigger_provider_operation_id: stale_attempt.trigger_provider_operation_id,
                failure_reason: plan.failure_reason,
                input_custody_snapshot: plan.input_custody_snapshot,
                created_at: now,
                updated_at: now,
            };
            let refreshed_attempt_row = sqlx_core::query::query(&format!(
                r#"
                INSERT INTO order_execution_attempts (
                    id,
                    order_id,
                    attempt_index,
                    attempt_kind,
                    status,
                    trigger_step_id,
                    trigger_provider_operation_id,
                    failure_reason_json,
                    input_custody_snapshot_json,
                    superseded_by_attempt_id,
                    superseded_reason_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, '{{}}'::jsonb, $10, $11)
                RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                "#
            ))
            .bind(refreshed_attempt.id)
            .bind(refreshed_attempt.order_id)
            .bind(refreshed_attempt.attempt_index)
            .bind(refreshed_attempt.attempt_kind.to_db_string())
            .bind(refreshed_attempt.status.to_db_string())
            .bind(refreshed_attempt.trigger_step_id)
            .bind(refreshed_attempt.trigger_provider_operation_id)
            .bind(refreshed_attempt.failure_reason.clone())
            .bind(refreshed_attempt.input_custody_snapshot.clone())
            .bind(refreshed_attempt.created_at)
            .bind(refreshed_attempt.updated_at)
            .fetch_one(&mut *tx)
            .await?;
            let refreshed_attempt = self.map_execution_attempt_row(&refreshed_attempt_row)?;

            let mut refreshed_legs = Vec::with_capacity(plan.legs.len());
            for mut leg in plan.legs {
                leg.order_id = order_id;
                leg.execution_attempt_id = Some(refreshed_attempt.id);
                leg.status = OrderExecutionStepStatus::Planned;
                leg.actual_amount_in = None;
                leg.actual_amount_out = None;
                leg.started_at = None;
                leg.completed_at = None;
                leg.created_at = now;
                leg.updated_at = now;
                set_json_value(
                    &mut leg.details,
                    "refreshed_from_attempt_id",
                    serde_json::json!(stale_attempt.id),
                );
                set_json_value(
                    &mut leg.details,
                    "refreshed_from_step_id",
                    serde_json::json!(failed_step.id),
                );
                insert_execution_leg_tx(&mut tx, &leg).await?;
                refreshed_legs.push(leg);
            }

            let known_leg_ids = refreshed_legs
                .iter()
                .map(|leg| leg.id)
                .collect::<std::collections::BTreeSet<_>>();
            let mut refreshed_steps = Vec::with_capacity(plan.steps.len());
            for mut step in plan.steps {
                if let Some(execution_leg_id) = step.execution_leg_id {
                    if !known_leg_ids.contains(&execution_leg_id) {
                        return Err(RouterCoreError::InvalidData {
                            message: format!(
                                "refreshed step {} references unmaterialized execution leg {}",
                                step.id, execution_leg_id
                            ),
                        });
                    }
                }
                step.order_id = order_id;
                step.execution_attempt_id = Some(refreshed_attempt.id);
                step.status = OrderExecutionStepStatus::Planned;
                step.tx_hash = None;
                step.provider_ref = None;
                step.idempotency_key = Some(format!(
                    "order:{order_id}:attempt:{refreshed_attempt_index}:{}:{}",
                    step.provider, step.step_index
                ));
                step.attempt_count = 0;
                step.next_attempt_at = None;
                step.started_at = None;
                step.completed_at = None;
                step.response = serde_json::json!({});
                step.error = serde_json::json!({});
                step.created_at = now;
                step.updated_at = now;
                set_json_value(
                    &mut step.details,
                    "refreshed_from_attempt_id",
                    serde_json::json!(stale_attempt.id),
                );
                set_json_value(
                    &mut step.details,
                    "refreshed_from_step_id",
                    serde_json::json!(failed_step.id),
                );
                insert_execution_step_tx(&mut tx, &step).await?;
                refreshed_steps.push(step);
            }

            sqlx_core::query::query(
                r#"
                UPDATE order_execution_steps
                SET
                    status = 'superseded',
                    error_json = $3,
                    completed_at = COALESCE(completed_at, $4),
                    updated_at = $4
                WHERE execution_attempt_id = $1
                  AND step_index >= $2
                  AND status IN ('failed', 'planned', 'ready', 'waiting', 'running')
                "#,
            )
            .bind(stale_attempt.id)
            .bind(failed_step.step_index)
            .bind(plan.superseded_reason.clone())
            .bind(now)
            .execute(&mut *tx)
            .await?;
            sqlx_core::query::query(
                r#"
                UPDATE order_execution_attempts
                SET
                    superseded_by_attempt_id = $2,
                    superseded_reason_json = $3,
                    updated_at = $4
                WHERE id = $1
                "#,
            )
            .bind(stale_attempt.id)
            .bind(refreshed_attempt.id)
            .bind(plan.superseded_reason)
            .bind(now)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            Ok::<ExecutionAttemptMaterializationRecord, RouterCoreError>(
                ExecutionAttemptMaterializationRecord {
                    order,
                    attempt: refreshed_attempt,
                    legs: refreshed_legs,
                    steps: refreshed_steps,
                },
            )
        }
        .await;
        telemetry::record_db_query(
            "order.create_refreshed_execution_attempt_from_failed_step",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn create_refund_attempt_from_funding_vault(
        &self,
        order_id: Uuid,
        failed_attempt_id: Uuid,
        plan: FundingVaultRefundAttemptPlan,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<ExecutionAttemptMaterializationRecord> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;

            let order_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                FOR UPDATE OF ro
                "#
            ))
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;
            if order.funding_vault_id != Some(plan.funding_vault_id) {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "order {} funding vault {:?} does not match refund funding vault {}",
                        order.id, order.funding_vault_id, plan.funding_vault_id
                    ),
                });
            }

            let failed_attempt_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE id = $1
                FOR UPDATE
                "#
            ))
            .bind(failed_attempt_id)
            .fetch_one(&mut *tx)
            .await?;
            let failed_attempt = self.map_execution_attempt_row(&failed_attempt_row)?;
            if failed_attempt.order_id != order_id {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "attempt {} does not belong to order {}",
                        failed_attempt.id, order_id
                    ),
                });
            }
            if failed_attempt.status != OrderExecutionAttemptStatus::Failed {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "attempt {} cannot start refund from {}",
                        failed_attempt.id,
                        failed_attempt.status.to_db_string()
                    ),
                });
            }

            let vault_status = sqlx_core::query_scalar::query_scalar::<_, String>(
                r#"
                SELECT status
                FROM deposit_vaults
                WHERE id = $1
                FOR UPDATE
                "#,
            )
            .bind(plan.funding_vault_id)
            .fetch_one(&mut *tx)
            .await?;
            let Some(vault_status) = DepositVaultStatus::from_db_string(&vault_status) else {
                return Err(RouterCoreError::InvalidData {
                    message: format!(
                        "unknown deposit vault status for vault {}",
                        plan.funding_vault_id
                    ),
                });
            };
            if matches!(
                vault_status,
                DepositVaultStatus::Completed
                    | DepositVaultStatus::Refunded
                    | DepositVaultStatus::ManualInterventionRequired
                    | DepositVaultStatus::RefundManualInterventionRequired
            ) {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "funding vault {} cannot materialize refund from {}",
                        plan.funding_vault_id,
                        vault_status.to_db_string()
                    ),
                });
            }

            let refund_attempt_index = failed_attempt.attempt_index + 1;
            if let Some(existing_refund_row) = sqlx_core::query::query(&format!(
                r#"
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE order_id = $1
                  AND attempt_index = $2
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                FOR UPDATE
                "#
            ))
            .bind(order_id)
            .bind(refund_attempt_index)
            .fetch_optional(&mut *tx)
            .await?
            {
                let attempt = self.map_execution_attempt_row(&existing_refund_row)?;
                if attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery {
                    return Err(RouterCoreError::Conflict {
                        message: format!(
                            "order {} attempt_index {} already materialized as {}",
                            order_id,
                            refund_attempt_index,
                            attempt.attempt_kind.to_db_string()
                        ),
                    });
                }
                let leg_rows = sqlx_core::query::query(&format!(
                    r#"
                    SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                    FROM order_execution_legs
                    WHERE execution_attempt_id = $1
                    ORDER BY leg_index ASC, created_at ASC, id ASC
                    "#
                ))
                .bind(attempt.id)
                .fetch_all(&mut *tx)
                .await?;
                let legs = leg_rows
                    .iter()
                    .map(|row| self.map_execution_leg_row(row))
                    .collect::<RouterCoreResult<Vec<_>>>()?;
                let step_rows = sqlx_core::query::query(&format!(
                    r#"
                    SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                    FROM order_execution_steps
                    WHERE execution_attempt_id = $1
                    ORDER BY step_index ASC, created_at ASC, id ASC
                    "#
                ))
                .bind(attempt.id)
                .fetch_all(&mut *tx)
                .await?;
                let steps = step_rows
                    .iter()
                    .map(|row| self.map_execution_step_row(row))
                    .collect::<RouterCoreResult<Vec<_>>>()?;
                tx.commit().await?;
                return Ok::<ExecutionAttemptMaterializationRecord, RouterCoreError>(
                    ExecutionAttemptMaterializationRecord {
                        order,
                        attempt,
                        legs,
                        steps,
                    },
                );
            }

            let refund_attempt = OrderExecutionAttempt {
                id: Uuid::now_v7(),
                order_id,
                attempt_index: refund_attempt_index,
                attempt_kind: OrderExecutionAttemptKind::RefundRecovery,
                status: OrderExecutionAttemptStatus::Active,
                trigger_step_id: failed_attempt.trigger_step_id,
                trigger_provider_operation_id: failed_attempt.trigger_provider_operation_id,
                failure_reason: plan.failure_reason,
                input_custody_snapshot: plan.input_custody_snapshot,
                created_at: now,
                updated_at: now,
            };
            let refund_attempt_row = sqlx_core::query::query(&format!(
                r#"
                INSERT INTO order_execution_attempts (
                    id,
                    order_id,
                    attempt_index,
                    attempt_kind,
                    status,
                    trigger_step_id,
                    trigger_provider_operation_id,
                    failure_reason_json,
                    input_custody_snapshot_json,
                    superseded_by_attempt_id,
                    superseded_reason_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, '{{}}'::jsonb, $10, $11)
                RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                "#
            ))
            .bind(refund_attempt.id)
            .bind(refund_attempt.order_id)
            .bind(refund_attempt.attempt_index)
            .bind(refund_attempt.attempt_kind.to_db_string())
            .bind(refund_attempt.status.to_db_string())
            .bind(refund_attempt.trigger_step_id)
            .bind(refund_attempt.trigger_provider_operation_id)
            .bind(refund_attempt.failure_reason.clone())
            .bind(refund_attempt.input_custody_snapshot.clone())
            .bind(refund_attempt.created_at)
            .bind(refund_attempt.updated_at)
            .fetch_one(&mut *tx)
            .await?;
            let refund_attempt = self.map_execution_attempt_row(&refund_attempt_row)?;

            let refund_leg = OrderExecutionLeg {
                id: Uuid::now_v7(),
                order_id,
                execution_attempt_id: Some(refund_attempt.id),
                transition_decl_id: None,
                leg_index: 0,
                leg_type: OrderExecutionStepType::Refund.to_db_string().to_string(),
                provider: "internal".to_string(),
                status: OrderExecutionStepStatus::Planned,
                input_asset: order.source_asset.clone(),
                output_asset: order.source_asset.clone(),
                amount_in: plan.amount.clone(),
                expected_amount_out: plan.amount.clone(),
                min_amount_out: None,
                actual_amount_in: None,
                actual_amount_out: None,
                started_at: None,
                completed_at: None,
                provider_quote_expires_at: None,
                details: serde_json::json!({
                    "schema_version": 1,
                    "refund_kind": "funding_vault_direct_transfer",
                    "quote_leg_count": 1,
                    "action_step_types": [OrderExecutionStepType::Refund.to_db_string()],
                }),
                usd_valuation: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            };
            insert_execution_leg_tx(&mut tx, &refund_leg).await?;

            let refund_step = OrderExecutionStep {
                id: Uuid::now_v7(),
                order_id,
                execution_attempt_id: Some(refund_attempt.id),
                execution_leg_id: Some(refund_leg.id),
                transition_decl_id: None,
                step_index: 0,
                step_type: OrderExecutionStepType::Refund,
                provider: "internal".to_string(),
                status: OrderExecutionStepStatus::Planned,
                input_asset: Some(order.source_asset.clone()),
                output_asset: Some(order.source_asset.clone()),
                amount_in: Some(plan.amount.clone()),
                min_amount_out: None,
                tx_hash: None,
                provider_ref: None,
                idempotency_key: Some(format!("order:{order_id}:refund:0")),
                attempt_count: 0,
                next_attempt_at: None,
                started_at: None,
                completed_at: None,
                details: serde_json::json!({
                    "schema_version": 1,
                    "refund_kind": "funding_vault_direct_transfer",
                    "source_custody_vault_id": plan.funding_vault_id,
                    "recipient_address": &order.refund_address,
                }),
                request: serde_json::json!({
                    "order_id": order.id,
                    "source_custody_vault_id": plan.funding_vault_id,
                    "recipient_address": &order.refund_address,
                    "amount": &plan.amount,
                }),
                response: serde_json::json!({}),
                error: serde_json::json!({}),
                usd_valuation: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            };
            insert_execution_step_tx(&mut tx, &refund_step).await?;

            tx.commit().await?;
            Ok::<ExecutionAttemptMaterializationRecord, RouterCoreError>(
                ExecutionAttemptMaterializationRecord {
                    order,
                    attempt: refund_attempt,
                    legs: vec![refund_leg],
                    steps: vec![refund_step],
                },
            )
        }
        .await;
        telemetry::record_db_query(
            "order.create_refund_attempt_from_funding_vault",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn mark_order_refund_required(
        &self,
        order_id: Uuid,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<RouterOrder> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH updated AS (
                UPDATE router_orders
                SET status = $2, updated_at = $3
                WHERE id = $1
                  AND status = $4
                RETURNING *
            )
            SELECT {ORDER_SELECT_COLUMNS}
            FROM updated ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            UNION ALL
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            WHERE ro.id = $1
              AND ro.status = $2
            LIMIT 1
            "#
        ))
        .bind(order_id)
        .bind(RouterOrderStatus::RefundRequired.to_db_string())
        .bind(now)
        .bind(RouterOrderStatus::Executing.to_db_string())
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.mark_order_refund_required",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;
        self.map_order_row(&row)
    }

    pub async fn mark_order_refunded(
        &self,
        order_id: Uuid,
        refund_attempt_id: Uuid,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<CompletedExecutionOrder> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let order_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                FOR UPDATE OF ro
                "#
            ))
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
            let locked_order = self.map_order_row(&order_row)?;

            if let Some(funding_vault_id) = locked_order.funding_vault_id {
                sqlx_core::query::query(
                    r#"
                    UPDATE deposit_vaults
                    SET status = $2, refunded_at = COALESCE(refunded_at, $3), updated_at = $3
                    WHERE id = $1
                      AND status = ANY($4)
                    "#,
                )
                .bind(funding_vault_id)
                .bind(DepositVaultStatus::Refunded.to_db_string())
                .bind(now)
                .bind(vec![
                    DepositVaultStatus::Funded.to_db_string(),
                    DepositVaultStatus::Executing.to_db_string(),
                    DepositVaultStatus::RefundRequired.to_db_string(),
                    DepositVaultStatus::Refunding.to_db_string(),
                ])
                .execute(&mut *tx)
                .await?;
            }

            let order_row = sqlx_core::query::query(&format!(
                r#"
                WITH updated AS (
                    UPDATE router_orders
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = ANY($4)
                    RETURNING *
                )
                SELECT {ORDER_SELECT_COLUMNS}
                FROM updated ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                UNION ALL
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                  AND ro.status = $2
                LIMIT 1
                "#
            ))
            .bind(order_id)
            .bind(RouterOrderStatus::Refunded.to_db_string())
            .bind(now)
            .bind(vec![
                RouterOrderStatus::Executing.to_db_string(),
                RouterOrderStatus::RefundRequired.to_db_string(),
                RouterOrderStatus::Refunding.to_db_string(),
            ])
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;

            let attempt_row = sqlx_core::query::query(&format!(
                r#"
                WITH updated AS (
                    UPDATE order_execution_attempts
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = $4
                    RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                )
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM updated
                UNION ALL
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE id = $1
                  AND status = $2
                LIMIT 1
                "#
            ))
            .bind(refund_attempt_id)
            .bind(OrderExecutionAttemptStatus::Completed.to_db_string())
            .bind(now)
            .bind(OrderExecutionAttemptStatus::Active.to_db_string())
            .fetch_one(&mut *tx)
            .await?;
            let attempt = self.map_execution_attempt_row(&attempt_row)?;
            if attempt.order_id != order_id {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "refund attempt {} does not belong to order {}",
                        attempt.id, order_id
                    ),
                });
            }
            if attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery {
                return Err(RouterCoreError::Conflict {
                    message: format!(
                        "attempt {} cannot mark order refunded because it is {}",
                        attempt.id,
                        attempt.attempt_kind.to_db_string()
                    ),
                });
            }

            tx.commit().await?;
            Ok::<CompletedExecutionOrder, RouterCoreError>(CompletedExecutionOrder {
                order,
                attempt,
            })
        }
        .await;
        telemetry::record_db_query(
            "order.mark_order_refunded",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn mark_order_refund_manual_intervention_required(
        &self,
        order_id: Uuid,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<RouterOrder> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let order_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                FOR UPDATE OF ro
                "#
            ))
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
            let locked_order = self.map_order_row(&order_row)?;

            if let Some(funding_vault_id) = locked_order.funding_vault_id {
                sqlx_core::query::query(
                    r#"
                    UPDATE deposit_vaults
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = ANY($4)
                    "#,
                )
                .bind(funding_vault_id)
                .bind(DepositVaultStatus::RefundManualInterventionRequired.to_db_string())
                .bind(now)
                .bind(vec![
                    DepositVaultStatus::Funded.to_db_string(),
                    DepositVaultStatus::Executing.to_db_string(),
                    DepositVaultStatus::RefundRequired.to_db_string(),
                    DepositVaultStatus::Refunding.to_db_string(),
                ])
                .execute(&mut *tx)
                .await?;
            }

            let order_row = sqlx_core::query::query(&format!(
                r#"
                WITH updated AS (
                    UPDATE router_orders
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = ANY($4)
                    RETURNING *
                )
                SELECT {ORDER_SELECT_COLUMNS}
                FROM updated ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                UNION ALL
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                  AND ro.status = $2
                LIMIT 1
                "#
            ))
            .bind(order_id)
            .bind(RouterOrderStatus::RefundManualInterventionRequired.to_db_string())
            .bind(now)
            .bind(vec![
                RouterOrderStatus::Executing.to_db_string(),
                RouterOrderStatus::RefundRequired.to_db_string(),
                RouterOrderStatus::Refunding.to_db_string(),
            ])
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;

            tx.commit().await?;
            Ok::<RouterOrder, RouterCoreError>(order)
        }
        .await;
        telemetry::record_db_query(
            "order.mark_order_refund_manual_intervention_required",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn create_refreshed_execution_attempt(
        &self,
        active_attempt_id: Uuid,
        refreshed_attempt: &OrderExecutionAttempt,
        superseded_reason: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionAttempt> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let superseded_row = sqlx_core::query::query(
                r#"
                UPDATE order_execution_attempts
                SET
                    status = 'superseded',
                    superseded_reason_json = $2,
                    updated_at = $3
                WHERE id = $1
                  AND status IN ('planning', 'active')
                RETURNING id
                "#,
            )
            .bind(active_attempt_id)
            .bind(superseded_reason.clone())
            .bind(updated_at)
            .fetch_optional(&mut *tx)
            .await?;
            if superseded_row.is_none() {
                return Err(RouterCoreError::NotFound);
            }

            let inserted = sqlx_core::query::query(&format!(
                r#"
                INSERT INTO order_execution_attempts (
                    id,
                    order_id,
                    attempt_index,
                    attempt_kind,
                    status,
                    trigger_step_id,
                    trigger_provider_operation_id,
                    failure_reason_json,
                    input_custody_snapshot_json,
                    superseded_by_attempt_id,
                    superseded_reason_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, '{{}}'::jsonb, $10, $11)
                RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                "#
            ))
            .bind(refreshed_attempt.id)
            .bind(refreshed_attempt.order_id)
            .bind(refreshed_attempt.attempt_index)
            .bind(refreshed_attempt.attempt_kind.to_db_string())
            .bind(refreshed_attempt.status.to_db_string())
            .bind(refreshed_attempt.trigger_step_id)
            .bind(refreshed_attempt.trigger_provider_operation_id)
            .bind(refreshed_attempt.failure_reason.clone())
            .bind(refreshed_attempt.input_custody_snapshot.clone())
            .bind(refreshed_attempt.created_at)
            .bind(refreshed_attempt.updated_at)
            .fetch_one(&mut *tx)
            .await?;

            sqlx_core::query::query(
                r#"
                UPDATE order_execution_attempts
                SET superseded_by_attempt_id = $2
                WHERE id = $1
                "#,
            )
            .bind(active_attempt_id)
            .bind(refreshed_attempt.id)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            self.map_execution_attempt_row(&inserted)
        }
        .await;
        telemetry::record_db_query(
            "order.create_refreshed_execution_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn get_execution_attempt(&self, id: Uuid) -> RouterCoreResult<OrderExecutionAttempt> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
            FROM order_execution_attempts
            WHERE id = $1
            "#
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_execution_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_execution_attempt_row(&row)
    }

    pub async fn get_execution_attempts(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderExecutionAttempt>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
            FROM order_execution_attempts
            WHERE order_id = $1
            ORDER BY attempt_index ASC, created_at ASC, id ASC
            "#
        ))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_execution_attempts",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_execution_attempt_row(row))
            .collect()
    }

    pub async fn get_latest_execution_attempt(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<Option<OrderExecutionAttempt>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
            FROM order_execution_attempts
            WHERE order_id = $1
            ORDER BY attempt_index DESC, created_at DESC, id DESC
            LIMIT 1
            "#
        ))
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_latest_execution_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        row.map(|row| self.map_execution_attempt_row(&row))
            .transpose()
    }

    pub async fn get_active_execution_attempt(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<Option<OrderExecutionAttempt>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
            FROM order_execution_attempts
            WHERE order_id = $1
              AND status IN ('planning', 'active')
            ORDER BY attempt_index DESC, created_at DESC, id DESC
            LIMIT 1
            "#
        ))
        .bind(order_id)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_active_execution_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        row.map(|row| self.map_execution_attempt_row(&row))
            .transpose()
    }

    pub async fn transition_execution_attempt_status(
        &self,
        id: Uuid,
        from_status: OrderExecutionAttemptStatus,
        to_status: OrderExecutionAttemptStatus,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionAttempt> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_attempts
            SET
                status = $2,
                updated_at = $4
            WHERE id = $1
              AND status = $3
            RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(to_status.to_db_string())
        .bind(from_status.to_db_string())
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.transition_execution_attempt_status",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_execution_attempt_row(&row)
    }

    pub async fn mark_execution_attempt_failed(
        &self,
        id: Uuid,
        trigger_step_id: Option<Uuid>,
        trigger_provider_operation_id: Option<Uuid>,
        failure_reason: serde_json::Value,
        input_custody_snapshot: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionAttempt> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_attempts
            SET
                status = 'failed',
                trigger_step_id = COALESCE($2, trigger_step_id),
                trigger_provider_operation_id = COALESCE($3, trigger_provider_operation_id),
                failure_reason_json = $4,
                input_custody_snapshot_json = $5,
                updated_at = $6
            WHERE id = $1
              AND status IN ('planning', 'active')
            RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(trigger_step_id)
        .bind(trigger_provider_operation_id)
        .bind(failure_reason)
        .bind(input_custody_snapshot)
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.mark_execution_attempt_failed",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_execution_attempt_row(&row)
    }

    pub async fn mark_execution_attempt_manual_intervention_required(
        &self,
        id: Uuid,
        failure_reason: serde_json::Value,
        input_custody_snapshot: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionAttempt> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_attempts
            SET
                status = $2,
                failure_reason_json = $3,
                input_custody_snapshot_json = $4,
                updated_at = $5
            WHERE id = $1
              AND status IN ('planning', 'active', 'failed', 'refund_required')
            RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(OrderExecutionAttemptStatus::ManualInterventionRequired.to_db_string())
        .bind(failure_reason)
        .bind(input_custody_snapshot)
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.mark_execution_attempt_manual_intervention_required",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_execution_attempt_row(&row)
    }

    pub async fn find_orders_pending_refund_planning(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN order_execution_attempts oea
              ON oea.order_id = ro.id
            WHERE ro.status = 'refund_required'
              AND oea.attempt_kind = 'refund_recovery'
              AND oea.status = 'refund_required'
              AND NOT EXISTS (
                  SELECT 1
                  FROM order_execution_attempts newer
                  WHERE newer.order_id = ro.id
                    AND newer.attempt_index > oea.attempt_index
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_orders_pending_refund_planning",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn find_refunding_orders_pending_direct_refund_finalization(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT DISTINCT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN deposit_vaults dv ON dv.id = ro.funding_vault_id
            LEFT JOIN order_execution_attempts oea
              ON oea.order_id = ro.id
             AND oea.attempt_kind = 'refund_recovery'
             AND oea.status = 'active'
            WHERE ro.status = 'refunding'
              AND dv.status = 'refunded'
              AND (
                  oea.id IS NULL
                  OR NOT EXISTS (
                      SELECT 1
                      FROM order_execution_steps oes
                      WHERE oes.execution_attempt_id = oea.id
                        AND oes.step_index > 0
                  )
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_refunding_orders_pending_direct_refund_finalization",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn find_orders_pending_manual_refund_vault_alignment(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT DISTINCT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN deposit_vaults dv ON dv.id = ro.funding_vault_id
            WHERE ro.status = 'refund_manual_intervention_required'
              AND dv.status IN ('refund_required', 'refunding')
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_orders_pending_manual_refund_vault_alignment",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn find_orders_with_manual_refund_funding_vault(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT DISTINCT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN deposit_vaults dv ON dv.id = ro.funding_vault_id
            WHERE ro.status IN ('refund_required', 'refunding')
              AND dv.status = 'refund_manual_intervention_required'
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_orders_with_manual_refund_funding_vault",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn find_orders_pending_retry_or_refund_decision(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            WHERE ro.status IN ('funded', 'executing', 'refunding')
              AND (
                    EXISTS (
                        SELECT 1
                        FROM order_execution_attempts active_attempt
                        JOIN order_execution_steps failed_step
                          ON failed_step.execution_attempt_id = active_attempt.id
                        WHERE active_attempt.order_id = ro.id
                          AND active_attempt.status IN ('planning', 'active')
                          AND failed_step.status = 'failed'
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM order_execution_attempts failed_attempt
                        WHERE failed_attempt.order_id = ro.id
                          AND failed_attempt.status = 'failed'
                          AND NOT EXISTS (
                              SELECT 1
                              FROM order_execution_attempts newer
                              WHERE newer.order_id = ro.id
                                AND newer.attempt_index > failed_attempt.attempt_index
                          )
                    )
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_orders_pending_retry_or_refund_decision",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn create_custody_vault(&self, vault: &CustodyVault) -> RouterCoreResult<()> {
        let started = Instant::now();
        let derivation_salt = vault.derivation_salt.as_ref().map(|salt| &salt[..]);
        let result = sqlx_core::query::query(
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
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
        )
        .bind(vault.id)
        .bind(vault.order_id)
        .bind(vault.role.to_db_string())
        .bind(vault.visibility.to_db_string())
        .bind(vault.chain.as_str())
        .bind(vault.asset.as_ref().map(|asset| asset.as_str()))
        .bind(&vault.address)
        .bind(vault.control_type.to_db_string())
        .bind(derivation_salt)
        .bind(vault.signer_ref.clone())
        .bind(vault.status.to_db_string())
        .bind(vault.metadata.clone())
        .bind(vault.created_at)
        .bind(vault.updated_at)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_custody_vault",
            result.is_ok(),
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    pub async fn get_custody_vaults(&self, order_id: Uuid) -> RouterCoreResult<Vec<CustodyVault>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {CUSTODY_VAULT_SELECT_COLUMNS}
            FROM custody_vaults
            WHERE order_id = $1
            ORDER BY created_at ASC, id ASC
            "#
        ))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_custody_vaults",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_custody_vault_row(row))
            .collect()
    }

    pub async fn get_custody_vault(&self, id: Uuid) -> RouterCoreResult<CustodyVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {CUSTODY_VAULT_SELECT_COLUMNS}
            FROM custody_vaults
            WHERE id = $1
            "#
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("order.get_custody_vault", result.is_ok(), started.elapsed());
        let row = result?;

        self.map_custody_vault_row(&row)
    }

    pub async fn patch_custody_vault_metadata(
        &self,
        vault_id: Uuid,
        metadata_patch: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<CustodyVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE custody_vaults
            SET
                metadata_json = COALESCE(metadata_json, '{{}}'::jsonb) || $2::jsonb,
                updated_at = $3
            WHERE id = $1
            RETURNING {CUSTODY_VAULT_SELECT_COLUMNS}
            "#
        ))
        .bind(vault_id)
        .bind(metadata_patch)
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.patch_custody_vault_metadata",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_custody_vault_row(&row)
    }

    pub async fn reactivate_internal_custody_vault(
        &self,
        vault_id: Uuid,
        metadata_patch: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<CustodyVault> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE custody_vaults
            SET
                status = 'active',
                metadata_json = COALESCE(metadata_json, '{{}}'::jsonb) || $2::jsonb,
                updated_at = $3
            WHERE id = $1
              AND visibility = 'internal'
              AND status = 'failed'
            RETURNING {CUSTODY_VAULT_SELECT_COLUMNS}
            "#
        ))
        .bind(vault_id)
        .bind(metadata_patch)
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.reactivate_internal_custody_vault",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_custody_vault_row(&row)
    }

    pub async fn finalize_internal_custody_vaults(
        &self,
        order_id: Uuid,
        status: CustodyVaultStatus,
        metadata_patch: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<Vec<CustodyVault>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE custody_vaults
            SET
                status = $2,
                metadata_json = COALESCE(metadata_json, '{{}}'::jsonb) || $3::jsonb,
                updated_at = $4
            WHERE order_id = $1
              AND visibility = 'internal'
              AND role <> 'source_deposit'
              AND status IN ('planned', 'active')
            RETURNING {CUSTODY_VAULT_SELECT_COLUMNS}
            "#
        ))
        .bind(order_id)
        .bind(status.to_db_string())
        .bind(metadata_patch)
        .bind(updated_at)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.finalize_internal_custody_vaults",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_custody_vault_row(row))
            .collect()
    }

    pub async fn get_terminal_orders_with_pending_internal_custody_finalization(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            WHERE ro.status IN (
                'completed',
                'refunded',
                'manual_intervention_required',
                'refund_manual_intervention_required',
                'expired'
            )
              AND EXISTS (
                    SELECT 1
                    FROM custody_vaults cv
                    WHERE cv.order_id = ro.id
                      AND cv.visibility = 'internal'
                      AND cv.role <> 'source_deposit'
                      AND cv.status IN ('planned', 'active')
              )
            ORDER BY ro.updated_at ASC, ro.id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_terminal_orders_with_pending_internal_custody_finalization",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter().map(|row| self.map_order_row(row)).collect()
    }

    pub async fn get_released_internal_custody_vaults_pending_sweep(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<CustodyVault>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {CUSTODY_VAULT_SELECT_COLUMNS}
            FROM custody_vaults
            WHERE visibility = 'internal'
              AND role <> 'source_deposit'
              AND status = 'released'
              AND COALESCE(metadata_json ->> 'release_sweep_terminal', 'false') <> 'true'
            ORDER BY updated_at ASC, created_at ASC, id ASC
            LIMIT $1
            "#
        ))
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_released_internal_custody_vaults_pending_sweep",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_custody_vault_row(row))
            .collect()
    }

    pub async fn create_provider_operation(
        &self,
        operation: &OrderProviderOperation,
    ) -> RouterCoreResult<()> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            INSERT INTO order_provider_operations (
                id,
                order_id,
                execution_attempt_id,
                execution_step_id,
                provider,
                operation_type,
                provider_ref,
                status,
                request_json,
                response_json,
                observed_state_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(operation.id)
        .bind(operation.order_id)
        .bind(operation.execution_attempt_id)
        .bind(operation.execution_step_id)
        .bind(&operation.provider)
        .bind(operation.operation_type.to_db_string())
        .bind(operation.provider_ref.clone())
        .bind(operation.status.to_db_string())
        .bind(operation.request.clone())
        .bind(operation.response.clone())
        .bind(operation.observed_state.clone())
        .bind(operation.created_at)
        .bind(operation.updated_at)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_provider_operation",
            result.is_ok(),
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    pub async fn upsert_provider_operation(
        &self,
        operation: &OrderProviderOperation,
    ) -> RouterCoreResult<Uuid> {
        let started = Instant::now();
        let result = sqlx_core::query_scalar::query_scalar(
            r#"
            WITH upserted AS (
                INSERT INTO order_provider_operations (
                    id,
                    order_id,
                    execution_attempt_id,
                    execution_step_id,
                    provider,
                    operation_type,
                    provider_ref,
                    status,
                    request_json,
                    response_json,
                    observed_state_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (execution_step_id) WHERE execution_step_id IS NOT NULL
                DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    execution_attempt_id = EXCLUDED.execution_attempt_id,
                    provider = EXCLUDED.provider,
                    operation_type = EXCLUDED.operation_type,
                    provider_ref = EXCLUDED.provider_ref,
                    status = EXCLUDED.status,
                    request_json = EXCLUDED.request_json,
                    response_json = EXCLUDED.response_json,
                    observed_state_json = EXCLUDED.observed_state_json,
                    updated_at = EXCLUDED.updated_at
                WHERE order_provider_operations.status NOT IN ('completed', 'failed', 'expired')
                  AND (
                    order_provider_operations.order_id IS DISTINCT FROM EXCLUDED.order_id
                    OR order_provider_operations.execution_attempt_id IS DISTINCT FROM EXCLUDED.execution_attempt_id
                    OR order_provider_operations.provider IS DISTINCT FROM EXCLUDED.provider
                    OR order_provider_operations.operation_type IS DISTINCT FROM EXCLUDED.operation_type
                    OR order_provider_operations.provider_ref IS DISTINCT FROM EXCLUDED.provider_ref
                    OR order_provider_operations.status IS DISTINCT FROM EXCLUDED.status
                    OR order_provider_operations.request_json IS DISTINCT FROM EXCLUDED.request_json
                    OR order_provider_operations.response_json IS DISTINCT FROM EXCLUDED.response_json
                    OR order_provider_operations.observed_state_json IS DISTINCT FROM EXCLUDED.observed_state_json
                  )
                RETURNING id
            )
            SELECT id FROM upserted
            UNION ALL
            SELECT id
            FROM order_provider_operations
            WHERE execution_step_id = $4
            LIMIT 1
            "#,
        )
        .bind(operation.id)
        .bind(operation.order_id)
        .bind(operation.execution_attempt_id)
        .bind(operation.execution_step_id)
        .bind(&operation.provider)
        .bind(operation.operation_type.to_db_string())
        .bind(operation.provider_ref.clone())
        .bind(operation.status.to_db_string())
        .bind(operation.request.clone())
        .bind(operation.response.clone())
        .bind(operation.observed_state.clone())
        .bind(operation.created_at)
        .bind(operation.updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.upsert_provider_operation",
            result.is_ok(),
            started.elapsed(),
        );
        Ok(result?)
    }

    pub async fn get_provider_operations(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderProviderOperation>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {PROVIDER_OPERATION_SELECT_COLUMNS}
            FROM order_provider_operations
            WHERE order_id = $1
            ORDER BY created_at ASC, id ASC
            "#
        ))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_provider_operations",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_provider_operation_row(row))
            .collect()
    }

    pub async fn get_provider_operation(
        &self,
        id: Uuid,
    ) -> RouterCoreResult<OrderProviderOperation> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {PROVIDER_OPERATION_SELECT_COLUMNS}
            FROM order_provider_operations
            WHERE id = $1
            "#
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_provider_operation",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_provider_operation_row(&row)
    }

    pub async fn find_terminal_provider_operations_pending_step_settlement(
        &self,
        limit: i64,
    ) -> RouterCoreResult<Vec<OrderProviderOperation>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            SELECT
                ops.id,
                ops.order_id,
                ops.execution_attempt_id,
                ops.execution_step_id,
                ops.provider,
                ops.operation_type,
                ops.provider_ref,
                ops.status,
                ops.request_json,
                ops.response_json,
                ops.observed_state_json,
                ops.created_at,
                ops.updated_at
            FROM order_provider_operations ops
            INNER JOIN order_execution_steps steps
                ON steps.id = ops.execution_step_id
            WHERE ops.status IN ('completed', 'failed', 'expired')
              AND steps.status IN ('running', 'waiting')
            ORDER BY ops.updated_at ASC, ops.created_at ASC, ops.id ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_terminal_provider_operations_pending_step_settlement",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_provider_operation_row(row))
            .collect()
    }

    pub async fn find_stale_running_execution_steps(
        &self,
        stale_before: DateTime<Utc>,
        limit: i64,
    ) -> RouterCoreResult<Vec<OrderExecutionStep>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            SELECT
                oes.id,
                oes.order_id,
                oes.execution_attempt_id,
                oes.execution_leg_id,
                oes.transition_decl_id,
                oes.step_index,
                oes.step_type,
                oes.provider,
                oes.status,
                oes.input_chain_id,
                oes.input_asset_id,
                oes.output_chain_id,
                oes.output_asset_id,
                oes.amount_in,
                oes.min_amount_out,
                oes.tx_hash,
                oes.provider_ref,
                oes.idempotency_key,
                oes.attempt_count,
                oes.next_attempt_at,
                oes.started_at,
                oes.completed_at,
                oes.details_json,
                oes.request_json,
                oes.response_json,
                oes.error_json,
                oes.usd_valuation_json,
                oes.created_at,
                oes.updated_at
            FROM order_execution_steps oes
            JOIN order_execution_attempts oea
              ON oea.id = oes.execution_attempt_id
             AND oea.status = 'active'
            JOIN router_orders ro
              ON ro.id = oes.order_id
            WHERE oes.status = 'running'
              AND oes.step_index > 0
              AND oes.updated_at < $1
              AND ro.status IN ('executing', 'refunding')
            ORDER BY oes.updated_at ASC, oes.created_at ASC, oes.id ASC
            LIMIT $2
            "#,
        )
        .bind(stale_before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.find_stale_running_execution_steps",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_execution_step_row(row))
            .collect()
    }

    pub async fn update_provider_operation_status(
        &self,
        id: Uuid,
        status: ProviderOperationStatus,
        provider_ref: Option<String>,
        observed_state: serde_json::Value,
        response: Option<serde_json::Value>,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<(OrderProviderOperation, bool)> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH existing AS (
                SELECT status AS previous_status
                FROM order_provider_operations
                WHERE id = $1
                FOR UPDATE
            )
            UPDATE order_provider_operations
            SET
                status = CASE
                    WHEN existing.previous_status IN ('completed', 'failed', 'expired') THEN order_provider_operations.status
                    ELSE $2
                END,
                provider_ref = CASE
                    WHEN existing.previous_status IN ('completed', 'failed', 'expired') THEN order_provider_operations.provider_ref
                    ELSE COALESCE($3, order_provider_operations.provider_ref)
                END,
                observed_state_json = CASE
                    WHEN existing.previous_status IN ('completed', 'failed', 'expired') THEN order_provider_operations.observed_state_json
                    ELSE $4
                END,
                response_json = CASE
                    WHEN existing.previous_status IN ('completed', 'failed', 'expired') THEN order_provider_operations.response_json
                    ELSE COALESCE($5, response_json)
                END,
                updated_at = CASE
                    WHEN existing.previous_status IN ('completed', 'failed', 'expired') THEN order_provider_operations.updated_at
                    ELSE $6
                END
            FROM existing
            WHERE id = $1
            RETURNING {PROVIDER_OPERATION_SELECT_COLUMNS},
                existing.previous_status
            "#
        ))
        .bind(id)
        .bind(status.to_db_string())
        .bind(provider_ref)
        .bind(observed_state)
        .bind(response)
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.update_provider_operation_status",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let operation = self.map_provider_operation_row(&row)?;
        let previous_status: String = row.get("previous_status");
        let previous_status = ProviderOperationStatus::from_db_string(&previous_status)
            .ok_or_else(|| RouterCoreError::InvalidData {
                message: format!(
                    "unknown provider operation status from database: {previous_status}"
                ),
            })?;
        Ok((
            operation,
            !matches!(
                previous_status,
                ProviderOperationStatus::Completed
                    | ProviderOperationStatus::Failed
                    | ProviderOperationStatus::Expired
            ),
        ))
    }

    pub async fn create_provider_operation_hint(
        &self,
        hint: &OrderProviderOperationHint,
    ) -> RouterCoreResult<OrderProviderOperationHint> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH upserted AS (
                INSERT INTO order_provider_operation_hints (
                    id,
                    provider_operation_id,
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
                    evidence_json = EXCLUDED.evidence_json,
                    status = EXCLUDED.status,
                    error_json = '{{}}'::jsonb,
                    claimed_at = NULL,
                    processed_at = NULL,
                    updated_at = EXCLUDED.updated_at
                WHERE order_provider_operation_hints.source = $13
                  AND order_provider_operation_hints.status IN ('ignored', 'failed')
                RETURNING {PROVIDER_OPERATION_HINT_SELECT_COLUMNS}
            )
            SELECT {PROVIDER_OPERATION_HINT_SELECT_COLUMNS}
            FROM upserted
            UNION ALL
            SELECT {PROVIDER_OPERATION_HINT_RETURNING_COLUMNS}
            FROM order_provider_operation_hints hints
            WHERE hints.source = $3
              AND hints.idempotency_key = $7
              AND NOT EXISTS (SELECT 1 FROM upserted)
            LIMIT 1
            "#
        ))
        .bind(hint.id)
        .bind(hint.provider_operation_id)
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
        .bind(PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_provider_operation_hint",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;
        let hint = self.map_provider_operation_hint_row(&row)?;

        let started = Instant::now();
        let result =
            sqlx_core::query::query("SELECT pg_notify('router_provider_operation_hints', $1)")
                .bind(hint.id.to_string())
                .execute(&self.pool)
                .await;
        telemetry::record_db_query(
            "order.notify_provider_operation_hint",
            result.is_ok(),
            started.elapsed(),
        );
        result?;

        Ok(hint)
    }

    pub async fn claim_pending_provider_operation_hints(
        &self,
        limit: i64,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<Vec<OrderProviderOperationHint>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH pending_candidates AS (
                SELECT id, created_at
                FROM order_provider_operation_hints
                WHERE status = 'pending'
                ORDER BY created_at ASC, id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            ),
            processing_candidates AS (
                SELECT id, created_at
                FROM order_provider_operation_hints
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
            UPDATE order_provider_operation_hints hints
            SET
                status = 'processing',
                claimed_at = $2,
                updated_at = $2
            FROM candidates
            WHERE hints.id = candidates.id
            RETURNING {PROVIDER_OPERATION_HINT_RETURNING_COLUMNS}
            "#
        ))
        .bind(limit)
        .bind(now)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.claim_pending_provider_operation_hints",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_provider_operation_hint_row(row))
            .collect()
    }

    pub async fn complete_provider_operation_hint(
        &self,
        id: Uuid,
        claimed_at: Option<DateTime<Utc>>,
        status: ProviderOperationHintStatus,
        error: serde_json::Value,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<OrderProviderOperationHint> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_provider_operation_hints
            SET
                status = $2,
                error_json = $3,
                processed_at = $4,
                updated_at = $4
            WHERE id = $1
              AND status = 'processing'
              AND claimed_at IS NOT DISTINCT FROM $5::timestamptz
            RETURNING {PROVIDER_OPERATION_HINT_SELECT_COLUMNS}
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
            "order.complete_provider_operation_hint",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_provider_operation_hint_row(&row)
    }

    pub async fn create_provider_address(
        &self,
        address: &OrderProviderAddress,
    ) -> RouterCoreResult<()> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            INSERT INTO order_provider_addresses (
                id,
                order_id,
                execution_step_id,
                provider_operation_id,
                provider,
                role,
                chain_id,
                asset_id,
                address,
                memo,
                expires_at,
                metadata_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            "#,
        )
        .bind(address.id)
        .bind(address.order_id)
        .bind(address.execution_step_id)
        .bind(address.provider_operation_id)
        .bind(&address.provider)
        .bind(address.role.to_db_string())
        .bind(address.chain.as_str())
        .bind(address.asset.as_ref().map(|asset| asset.as_str()))
        .bind(&address.address)
        .bind(address.memo.clone())
        .bind(address.expires_at)
        .bind(address.metadata.clone())
        .bind(address.created_at)
        .bind(address.updated_at)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_provider_address",
            result.is_ok(),
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    pub async fn upsert_provider_address(
        &self,
        address: &OrderProviderAddress,
    ) -> RouterCoreResult<Uuid> {
        let started = Instant::now();
        let query = if address.execution_step_id.is_some() {
            r#"
            WITH upserted AS (
                INSERT INTO order_provider_addresses (
                    id,
                    order_id,
                    execution_step_id,
                    provider_operation_id,
                    provider,
                    role,
                    chain_id,
                    asset_id,
                    address,
                    memo,
                    expires_at,
                    metadata_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (execution_step_id, provider, role, chain_id, address)
                WHERE execution_step_id IS NOT NULL
                DO UPDATE SET
                    provider_operation_id = COALESCE(
                        EXCLUDED.provider_operation_id,
                        order_provider_addresses.provider_operation_id
                    ),
                    asset_id = EXCLUDED.asset_id,
                    memo = EXCLUDED.memo,
                    expires_at = EXCLUDED.expires_at,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = EXCLUDED.updated_at
                WHERE order_provider_addresses.updated_at <= EXCLUDED.updated_at
                RETURNING id
            )
            SELECT id
            FROM upserted
            UNION ALL
            SELECT id
            FROM order_provider_addresses
            WHERE execution_step_id = $3
              AND provider = $5
              AND role = $6
              AND chain_id = $7
              AND address = $9
              AND NOT EXISTS (SELECT 1 FROM upserted)
            LIMIT 1
            "#
        } else {
            r#"
            WITH upserted AS (
                INSERT INTO order_provider_addresses (
                    id,
                    order_id,
                    execution_step_id,
                    provider_operation_id,
                    provider,
                    role,
                    chain_id,
                    asset_id,
                    address,
                    memo,
                    expires_at,
                    metadata_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (order_id, provider, role, chain_id, address)
                WHERE execution_step_id IS NULL
                DO UPDATE SET
                    provider_operation_id = COALESCE(
                        EXCLUDED.provider_operation_id,
                        order_provider_addresses.provider_operation_id
                    ),
                    asset_id = EXCLUDED.asset_id,
                    memo = EXCLUDED.memo,
                    expires_at = EXCLUDED.expires_at,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = EXCLUDED.updated_at
                WHERE order_provider_addresses.updated_at <= EXCLUDED.updated_at
                RETURNING id
            )
            SELECT id
            FROM upserted
            UNION ALL
            SELECT id
            FROM order_provider_addresses
            WHERE order_id = $2
              AND execution_step_id IS NULL
              AND provider = $5
              AND role = $6
              AND chain_id = $7
              AND address = $9
              AND NOT EXISTS (SELECT 1 FROM upserted)
            LIMIT 1
            "#
        };
        let result = sqlx_core::query_scalar::query_scalar(query)
            .bind(address.id)
            .bind(address.order_id)
            .bind(address.execution_step_id)
            .bind(address.provider_operation_id)
            .bind(&address.provider)
            .bind(address.role.to_db_string())
            .bind(address.chain.as_str())
            .bind(address.asset.as_ref().map(|asset| asset.as_str()))
            .bind(&address.address)
            .bind(address.memo.clone())
            .bind(address.expires_at)
            .bind(address.metadata.clone())
            .bind(address.created_at)
            .bind(address.updated_at)
            .fetch_one(&self.pool)
            .await;
        telemetry::record_db_query(
            "order.upsert_provider_address",
            result.is_ok(),
            started.elapsed(),
        );
        Ok(result?)
    }

    pub async fn get_provider_addresses_by_operation(
        &self,
        provider_operation_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderProviderAddress>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {PROVIDER_ADDRESS_SELECT_COLUMNS}
            FROM order_provider_addresses
            WHERE provider_operation_id = $1
            ORDER BY created_at ASC, id ASC
            "#
        ))
        .bind(provider_operation_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_provider_addresses_by_operation",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_provider_address_row(row))
            .collect()
    }

    pub async fn get_provider_addresses(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderProviderAddress>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {PROVIDER_ADDRESS_SELECT_COLUMNS}
            FROM order_provider_addresses
            WHERE order_id = $1
            ORDER BY created_at ASC, id ASC
            "#
        ))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_provider_addresses",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_provider_address_row(row))
            .collect()
    }

    pub async fn create_execution_legs_idempotent(
        &self,
        legs: &[OrderExecutionLeg],
    ) -> RouterCoreResult<u64> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let mut inserted = 0_u64;

            for leg in legs {
                let query = if leg.execution_attempt_id.is_some() {
                    r#"
                    INSERT INTO order_execution_legs (
                        id,
                        order_id,
                        execution_attempt_id,
                        transition_decl_id,
                        leg_index,
                        leg_type,
                        provider,
                        status,
                        input_chain_id,
                        input_asset_id,
                        output_chain_id,
                        output_asset_id,
                        amount_in,
                        expected_amount_out,
                        min_amount_out,
                        actual_amount_in,
                        actual_amount_out,
                        started_at,
                        completed_at,
                        provider_quote_expires_at,
                        details_json,
                        usd_valuation_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
                    )
                    ON CONFLICT (execution_attempt_id, leg_index)
                    WHERE execution_attempt_id IS NOT NULL DO NOTHING
                    "#
                } else {
                    r#"
                    INSERT INTO order_execution_legs (
                        id,
                        order_id,
                        execution_attempt_id,
                        transition_decl_id,
                        leg_index,
                        leg_type,
                        provider,
                        status,
                        input_chain_id,
                        input_asset_id,
                        output_chain_id,
                        output_asset_id,
                        amount_in,
                        expected_amount_out,
                        min_amount_out,
                        actual_amount_in,
                        actual_amount_out,
                        started_at,
                        completed_at,
                        provider_quote_expires_at,
                        details_json,
                        usd_valuation_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
                    )
                    ON CONFLICT (order_id, leg_index)
                    WHERE execution_attempt_id IS NULL DO NOTHING
                    "#
                };
                let result = sqlx_core::query::query(query)
                    .bind(leg.id)
                    .bind(leg.order_id)
                    .bind(leg.execution_attempt_id)
                    .bind(leg.transition_decl_id.clone())
                    .bind(leg.leg_index)
                    .bind(&leg.leg_type)
                    .bind(&leg.provider)
                    .bind(leg.status.to_db_string())
                    .bind(leg.input_asset.chain.as_str())
                    .bind(leg.input_asset.asset.as_str())
                    .bind(leg.output_asset.chain.as_str())
                    .bind(leg.output_asset.asset.as_str())
                    .bind(&leg.amount_in)
                    .bind(&leg.expected_amount_out)
                    .bind(leg.min_amount_out.clone())
                    .bind(leg.actual_amount_in.clone())
                    .bind(leg.actual_amount_out.clone())
                    .bind(leg.started_at)
                    .bind(leg.completed_at)
                    .bind(leg.provider_quote_expires_at)
                    .bind(leg.details.clone())
                    .bind(leg.usd_valuation.clone())
                    .bind(leg.created_at)
                    .bind(leg.updated_at)
                    .execute(&mut *tx)
                    .await?;
                match result.rows_affected() {
                    1 => inserted += 1,
                    0 => {
                        let existing_row = if let Some(execution_attempt_id) =
                            leg.execution_attempt_id
                        {
                            sqlx_core::query::query(&format!(
                                r#"
                                SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                                FROM order_execution_legs
                                WHERE execution_attempt_id = $1
                                  AND leg_index = $2
                                "#
                            ))
                            .bind(execution_attempt_id)
                            .bind(leg.leg_index)
                            .fetch_optional(&mut *tx)
                            .await?
                        } else {
                            sqlx_core::query::query(&format!(
                                r#"
                                SELECT {EXECUTION_LEG_SELECT_COLUMNS}
                                FROM order_execution_legs
                                WHERE order_id = $1
                                  AND leg_index = $2
                                  AND execution_attempt_id IS NULL
                                "#
                            ))
                            .bind(leg.order_id)
                            .bind(leg.leg_index)
                            .fetch_optional(&mut *tx)
                            .await?
                        };
                        let Some(existing_row) = existing_row else {
                            return Err(RouterCoreError::InvalidData {
                                message: format!(
                                    "execution leg idempotency conflict did not return existing row for order {} leg_index {}",
                                    leg.order_id, leg.leg_index
                                ),
                            });
                        };
                        let existing = self.map_execution_leg_row(&existing_row)?;
                        ensure_execution_leg_plan_matches(&existing, leg)?;
                    }
                    rows => {
                        return Err(RouterCoreError::InvalidData {
                            message: format!(
                                "execution leg insert affected unexpected row count {rows} for order {} leg_index {}",
                                leg.order_id, leg.leg_index
                            ),
                        });
                    }
                }
            }

            tx.commit().await?;
            Ok::<u64, RouterCoreError>(inserted)
        }
        .await;
        telemetry::record_db_query(
            "order.create_execution_legs_idempotent",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn create_execution_step(&self, step: &OrderExecutionStep) -> RouterCoreResult<()> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            INSERT INTO order_execution_steps (
                id,
                order_id,
                execution_attempt_id,
                execution_leg_id,
                transition_decl_id,
                step_index,
                step_type,
                provider,
                status,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id,
                amount_in,
                min_amount_out,
                tx_hash,
                provider_ref,
                idempotency_key,
                attempt_count,
                next_attempt_at,
                started_at,
                completed_at,
                details_json,
                request_json,
                response_json,
                error_json,
                usd_valuation_json,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
                $23, $24, $25, $26, $27, $28, $29
            )
            "#,
        )
        .bind(step.id)
        .bind(step.order_id)
        .bind(step.execution_attempt_id)
        .bind(step.execution_leg_id)
        .bind(step.transition_decl_id.clone())
        .bind(step.step_index)
        .bind(step.step_type.to_db_string())
        .bind(&step.provider)
        .bind(step.status.to_db_string())
        .bind(step.input_asset.as_ref().map(|asset| asset.chain.as_str()))
        .bind(step.input_asset.as_ref().map(|asset| asset.asset.as_str()))
        .bind(step.output_asset.as_ref().map(|asset| asset.chain.as_str()))
        .bind(step.output_asset.as_ref().map(|asset| asset.asset.as_str()))
        .bind(step.amount_in.clone())
        .bind(step.min_amount_out.clone())
        .bind(step.tx_hash.clone())
        .bind(step.provider_ref.clone())
        .bind(step.idempotency_key.clone())
        .bind(step.attempt_count)
        .bind(step.next_attempt_at)
        .bind(step.started_at)
        .bind(step.completed_at)
        .bind(step.details.clone())
        .bind(step.request.clone())
        .bind(step.response.clone())
        .bind(step.error.clone())
        .bind(step.usd_valuation.clone())
        .bind(step.created_at)
        .bind(step.updated_at)
        .execute(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.create_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    pub async fn create_execution_steps_idempotent(
        &self,
        steps: &[OrderExecutionStep],
    ) -> RouterCoreResult<u64> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let mut inserted = 0_u64;

            for step in steps {
                let query = if step.execution_attempt_id.is_some() {
                    r#"
                    INSERT INTO order_execution_steps (
                        id,
                        order_id,
                        execution_attempt_id,
                        execution_leg_id,
                        transition_decl_id,
                        step_index,
                        step_type,
                        provider,
                        status,
                        input_chain_id,
                        input_asset_id,
                        output_chain_id,
                        output_asset_id,
                        amount_in,
                        min_amount_out,
                        tx_hash,
                        provider_ref,
                        idempotency_key,
                        attempt_count,
                        next_attempt_at,
                        started_at,
                        completed_at,
                        details_json,
                        request_json,
                        response_json,
                        error_json,
                        usd_valuation_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
                        $23, $24, $25, $26, $27, $28, $29
                    )
                    ON CONFLICT (execution_attempt_id, step_index)
                    WHERE execution_attempt_id IS NOT NULL DO NOTHING
                    "#
                } else {
                    r#"
                    INSERT INTO order_execution_steps (
                        id,
                        order_id,
                        execution_attempt_id,
                        execution_leg_id,
                        transition_decl_id,
                        step_index,
                        step_type,
                        provider,
                        status,
                        input_chain_id,
                        input_asset_id,
                        output_chain_id,
                        output_asset_id,
                        amount_in,
                        min_amount_out,
                        tx_hash,
                        provider_ref,
                        idempotency_key,
                        attempt_count,
                        next_attempt_at,
                        started_at,
                        completed_at,
                        details_json,
                        request_json,
                        response_json,
                        error_json,
                        usd_valuation_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
                        $23, $24, $25, $26, $27, $28, $29
                    )
                    ON CONFLICT (order_id, step_index)
                    WHERE execution_attempt_id IS NULL DO NOTHING
                    "#
                };
                let result = sqlx_core::query::query(query)
                    .bind(step.id)
                    .bind(step.order_id)
                    .bind(step.execution_attempt_id)
                    .bind(step.execution_leg_id)
                    .bind(step.transition_decl_id.clone())
                    .bind(step.step_index)
                    .bind(step.step_type.to_db_string())
                    .bind(&step.provider)
                    .bind(step.status.to_db_string())
                    .bind(step.input_asset.as_ref().map(|asset| asset.chain.as_str()))
                    .bind(step.input_asset.as_ref().map(|asset| asset.asset.as_str()))
                    .bind(step.output_asset.as_ref().map(|asset| asset.chain.as_str()))
                    .bind(step.output_asset.as_ref().map(|asset| asset.asset.as_str()))
                    .bind(step.amount_in.clone())
                    .bind(step.min_amount_out.clone())
                    .bind(step.tx_hash.clone())
                    .bind(step.provider_ref.clone())
                    .bind(step.idempotency_key.clone())
                    .bind(step.attempt_count)
                    .bind(step.next_attempt_at)
                    .bind(step.started_at)
                    .bind(step.completed_at)
                    .bind(step.details.clone())
                    .bind(step.request.clone())
                    .bind(step.response.clone())
                    .bind(step.error.clone())
                    .bind(step.usd_valuation.clone())
                    .bind(step.created_at)
                    .bind(step.updated_at)
                    .execute(&mut *tx)
                    .await?;
                match result.rows_affected() {
                    1 => inserted += 1,
                    0 => {
                        let existing_row = if let Some(execution_attempt_id) =
                            step.execution_attempt_id
                        {
                            sqlx_core::query::query(&format!(
                                r#"
                                SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                                FROM order_execution_steps
                                WHERE execution_attempt_id = $1
                                  AND step_index = $2
                                "#
                            ))
                            .bind(execution_attempt_id)
                            .bind(step.step_index)
                            .fetch_optional(&mut *tx)
                            .await?
                        } else {
                            sqlx_core::query::query(&format!(
                                r#"
                                SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                                FROM order_execution_steps
                                WHERE order_id = $1
                                  AND step_index = $2
                                  AND execution_attempt_id IS NULL
                                "#
                            ))
                            .bind(step.order_id)
                            .bind(step.step_index)
                            .fetch_optional(&mut *tx)
                            .await?
                        };
                        let Some(existing_row) = existing_row else {
                            return Err(RouterCoreError::InvalidData {
                                message: format!(
                                    "execution step idempotency conflict did not return existing row for order {} step_index {}",
                                    step.order_id, step.step_index
                                ),
                            });
                        };
                        let existing = self.map_execution_step_row(&existing_row)?;
                        ensure_execution_step_plan_matches(&existing, step)?;
                    }
                    rows => {
                        return Err(RouterCoreError::InvalidData {
                            message: format!(
                                "execution step insert affected unexpected row count {rows} for order {} step_index {}",
                                step.order_id, step.step_index
                            ),
                        });
                    }
                }
            }

            tx.commit().await?;
            Ok::<u64, RouterCoreError>(inserted)
        }
        .await;
        telemetry::record_db_query(
            "order.create_execution_steps_idempotent",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn get_execution_steps_for_attempt(
        &self,
        execution_attempt_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderExecutionStep>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_STEP_SELECT_COLUMNS}
            FROM order_execution_steps
            WHERE execution_attempt_id = $1
            ORDER BY step_index ASC, created_at ASC, id ASC
            "#
        ))
        .bind(execution_attempt_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_execution_steps_for_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_execution_step_row(row))
            .collect()
    }

    pub async fn materialize_execution_step_request(
        &self,
        id: Uuid,
        expected_status: OrderExecutionStepStatus,
        request: serde_json::Value,
        details: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                request_json = $2,
                details_json = $3,
                updated_at = $4
            WHERE id = $1
              AND status = $5
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(request)
        .bind(details)
        .bind(updated_at)
        .bind(expected_status.to_db_string())
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.materialize_execution_step_request",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_execution_step_row(&row)
    }

    pub async fn get_execution_legs_for_attempt(
        &self,
        execution_attempt_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderExecutionLeg>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_LEG_SELECT_COLUMNS}
            FROM order_execution_legs
            WHERE execution_attempt_id = $1
            ORDER BY leg_index ASC, created_at ASC, id ASC
            "#
        ))
        .bind(execution_attempt_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_execution_legs_for_attempt",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_execution_leg_row(row))
            .collect()
    }

    pub async fn get_execution_steps(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderExecutionStep>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_STEP_SELECT_COLUMNS}
            FROM order_execution_steps
            WHERE order_id = $1
            ORDER BY
                COALESCE(
                    (
                        SELECT attempt_index
                        FROM order_execution_attempts attempts
                        WHERE attempts.id = order_execution_steps.execution_attempt_id
                    ),
                    0
                ) ASC,
                step_index ASC,
                created_at ASC,
                id ASC
            "#
        ))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_execution_steps",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_execution_step_row(row))
            .collect()
    }

    pub async fn get_execution_legs(
        &self,
        order_id: Uuid,
    ) -> RouterCoreResult<Vec<OrderExecutionLeg>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_LEG_SELECT_COLUMNS}
            FROM order_execution_legs
            WHERE order_id = $1
            ORDER BY
                COALESCE(
                    (
                        SELECT attempt_index
                        FROM order_execution_attempts attempts
                        WHERE attempts.id = order_execution_legs.execution_attempt_id
                    ),
                    0
                ) ASC,
                leg_index ASC,
                created_at ASC,
                id ASC
            "#
        ))
        .bind(order_id)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_execution_legs",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        rows.iter()
            .map(|row| self.map_execution_leg_row(row))
            .collect()
    }

    pub async fn get_execution_step(&self, id: Uuid) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {EXECUTION_STEP_SELECT_COLUMNS}
            FROM order_execution_steps
            WHERE id = $1
            "#
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_execution_step_row(&row)
    }

    pub async fn refresh_execution_leg_from_actions(
        &self,
        execution_leg_id: Uuid,
    ) -> RouterCoreResult<Option<OrderExecutionLeg>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH current_actions AS (
                SELECT DISTINCT ON (steps.step_index)
                    steps.*,
                    COALESCE(attempts.attempt_index, 0) AS attempt_index
                FROM order_execution_steps steps
                LEFT JOIN order_execution_attempts attempts
                  ON attempts.id = steps.execution_attempt_id
                WHERE steps.execution_leg_id = $1
                ORDER BY
                    steps.step_index,
                    COALESCE(attempts.attempt_index, 0) DESC,
                    steps.updated_at DESC,
                    steps.id DESC
            ),
            latest_attempt AS (
                SELECT
                    steps.execution_attempt_id
                FROM order_execution_steps steps
                LEFT JOIN order_execution_attempts attempts
                  ON attempts.id = steps.execution_attempt_id
                WHERE steps.execution_leg_id = $1
                  AND steps.execution_attempt_id IS NOT NULL
                ORDER BY
                    COALESCE(attempts.attempt_index, 0) DESC,
                    steps.updated_at DESC,
                    steps.id DESC
                LIMIT 1
            ),
            action_summary AS (
                SELECT
                    COUNT(*) AS total_actions,
                    BOOL_OR(status = 'failed') AS has_failed,
                    BOOL_OR(status = 'cancelled') AS has_cancelled,
                    BOOL_OR(status = 'superseded') AS has_superseded,
                    BOOL_OR(status = 'running') AS has_running,
                    BOOL_OR(status = 'waiting') AS has_waiting,
                    BOOL_OR(status = 'ready') AS has_ready,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed_actions,
                    COUNT(*) FILTER (WHERE status IN ('completed', 'skipped', 'superseded')) AS terminal_actions,
                    MIN(started_at) FILTER (WHERE started_at IS NOT NULL) AS first_started_at,
                    MAX(completed_at) FILTER (WHERE completed_at IS NOT NULL) AS last_completed_at
                FROM current_actions
            ),
            first_success_action AS (
                SELECT
                    response_json,
                    request_json,
                    amount_in
                FROM current_actions
                WHERE status IN ('completed', 'skipped')
                ORDER BY step_index ASC, updated_at ASC, id ASC
                LIMIT 1
            ),
            last_success_action AS (
                SELECT
                    response_json,
                    request_json,
                    amount_in,
                    min_amount_out
                FROM current_actions
                WHERE status IN ('completed', 'skipped')
                ORDER BY step_index DESC, updated_at DESC, id DESC
                LIMIT 1
            ),
            rolled_up AS (
                SELECT
                    CASE
                        WHEN action_summary.has_failed THEN 'failed'
                        WHEN action_summary.has_cancelled THEN 'cancelled'
                        WHEN action_summary.total_actions > 0
                             AND action_summary.completed_actions = action_summary.total_actions
                            THEN 'completed'
                        WHEN action_summary.total_actions > 0
                             AND action_summary.terminal_actions = action_summary.total_actions
                             AND action_summary.has_superseded
                            THEN 'superseded'
                        WHEN action_summary.total_actions > 0
                             AND action_summary.terminal_actions = action_summary.total_actions
                            THEN 'skipped'
                        WHEN action_summary.has_running THEN 'running'
                        WHEN action_summary.has_waiting THEN 'waiting'
                        WHEN action_summary.has_ready THEN 'ready'
                        ELSE 'planned'
                    END AS status,
                    action_summary.total_actions > 0
                        AND action_summary.completed_actions = action_summary.total_actions
                        AS completed,
                    action_summary.first_started_at,
                    action_summary.last_completed_at,
                    (SELECT execution_attempt_id FROM latest_attempt) AS latest_execution_attempt_id,
                    first_success_action.response_json AS first_response_json,
                    first_success_action.request_json AS first_request_json,
                    first_success_action.amount_in AS first_amount_in,
                    last_success_action.response_json AS last_response_json,
                    last_success_action.request_json AS last_request_json,
                    last_success_action.amount_in AS last_amount_in,
                    last_success_action.min_amount_out AS last_min_amount_out
                FROM action_summary
                LEFT JOIN first_success_action ON true
                LEFT JOIN last_success_action ON true
            )
            UPDATE order_execution_legs leg
            SET
                execution_attempt_id = COALESCE(
                    rolled_up.latest_execution_attempt_id,
                    leg.execution_attempt_id
                ),
                status = rolled_up.status,
                started_at = COALESCE(leg.started_at, rolled_up.first_started_at),
                completed_at = CASE
                    WHEN rolled_up.status IN ('completed', 'failed', 'cancelled')
                        THEN COALESCE(rolled_up.last_completed_at, leg.completed_at, now())
                    ELSE NULL
                END,
                actual_amount_in = CASE
                    WHEN rolled_up.completed THEN COALESCE(
                        CASE
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,source_amount}}') ~ '^[0-9]+$'
                                THEN rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,source_amount}}'
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,sourceAmount}}') ~ '^[0-9]+$'
                                THEN rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,sourceAmount}}'
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.first_response_json->>'amount') ~ '^[0-9]+$'
                                THEN rolled_up.first_response_json->>'amount'
                        END,
                        (
                            SELECT probe.value->>'debit_delta'
                            FROM jsonb_array_elements(
                                CASE
                                    WHEN jsonb_typeof(rolled_up.first_response_json #> '{{balance_observation,probes}}') = 'array'
                                        THEN rolled_up.first_response_json #> '{{balance_observation,probes}}'
                                    ELSE '[]'::jsonb
                                END
                            ) AS probe(value)
                            WHERE probe.value->>'role' = 'source'
                              AND probe.value->>'debit_delta' ~ '^[0-9]+$'
                              AND (probe.value->>'debit_delta')::numeric > 0
                            LIMIT 1
                        ),
                        rolled_up.first_response_json->>'amount_in',
                        rolled_up.first_response_json->>'amountIn',
                        rolled_up.first_response_json->>'input_amount',
                        rolled_up.first_response_json->>'inputAmount',
                        rolled_up.first_response_json #>> '{{response,amount_in}}',
                        rolled_up.first_response_json #>> '{{response,amountIn}}',
                        rolled_up.first_response_json #>> '{{response,input_amount}}',
                        rolled_up.first_response_json #>> '{{response,inputAmount}}',
                        rolled_up.first_response_json #>> '{{observed_state,amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,amountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,actual_amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,actualAmountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,input_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,inputAmount}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,amountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,actual_amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,actualAmountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,input_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,inputAmount}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,amountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,actual_amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,actualAmountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,input_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,inputAmount}}',
                        rolled_up.first_response_json #>> '{{provider_context,amount_in}}',
                        rolled_up.first_response_json #>> '{{provider_context,amountIn}}',
                        rolled_up.first_response_json #>> '{{provider_context,input_amount}}',
                        rolled_up.first_response_json #>> '{{provider_context,inputAmount}}',
                        rolled_up.first_response_json #>> '{{provider_context,amount}}',
                        rolled_up.first_request_json->>'amount_in',
                        rolled_up.first_request_json->>'amountIn',
                        rolled_up.first_request_json->>'input_amount',
                        rolled_up.first_request_json->>'inputAmount',
                        rolled_up.first_request_json->>'amount',
                        rolled_up.first_amount_in,
                        rolled_up.first_response_json #>> '{{observed_state,amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,amountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,input_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,inputAmount}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,amountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,input_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,inputAmount}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,source_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,previous_observed_state,sourceAmount}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,amount_in}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,amountIn}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,input_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,inputAmount}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,source_amount}}',
                        rolled_up.first_response_json #>> '{{observed_state,provider_observed_state,sourceAmount}}'
                    )
                    ELSE NULL
                END,
                actual_amount_out = CASE
                    WHEN rolled_up.completed THEN COALESCE(
                        (
                            SELECT probe.value->>'credit_delta'
                            FROM jsonb_array_elements(
                                CASE
                                    WHEN jsonb_typeof(rolled_up.last_response_json #> '{{balance_observation,probes}}') = 'array'
                                        THEN rolled_up.last_response_json #> '{{balance_observation,probes}}'
                                    ELSE '[]'::jsonb
                                END
                            ) AS probe(value)
                            WHERE probe.value->>'role' = 'destination'
                              AND probe.value->>'credit_delta' ~ '^[0-9]+$'
                              AND (probe.value->>'credit_delta')::numeric > 0
                            LIMIT 1
                        ),
                        CASE
                            WHEN leg.leg_type = 'cctp_bridge' THEN (
                                SELECT probe.value->>'credit_delta'
                                FROM jsonb_array_elements(
                                    CASE
                                        WHEN jsonb_typeof(rolled_up.last_response_json #> '{{balance_observation,probes}}') = 'array'
                                            THEN rolled_up.last_response_json #> '{{balance_observation,probes}}'
                                        ELSE '[]'::jsonb
                                    END
                                ) AS probe(value)
                                WHERE probe.value->>'role' = 'source'
                                  AND probe.value->>'credit_delta' ~ '^[0-9]+$'
                                  AND (probe.value->>'credit_delta')::numeric > 0
                                LIMIT 1
                            )
                        END,
                        rolled_up.last_response_json->>'amount_out',
                        rolled_up.last_response_json->>'amountOut',
                        rolled_up.last_response_json->>'output_amount',
                        rolled_up.last_response_json->>'outputAmount',
                        rolled_up.last_response_json->>'expectedOutputAmount',
                        rolled_up.last_response_json->>'minOutputAmount',
                        rolled_up.last_response_json #>> '{{response,amount_out}}',
                        rolled_up.last_response_json #>> '{{response,amountOut}}',
                        rolled_up.last_response_json #>> '{{response,output_amount}}',
                        rolled_up.last_response_json #>> '{{response,outputAmount}}',
                        rolled_up.last_response_json #>> '{{response,expectedOutputAmount}}',
                        rolled_up.last_response_json #>> '{{response,minOutputAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,amountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,actual_amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,actualAmountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,output_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,outputAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,amountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,actual_amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,actualAmountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,output_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,outputAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,amountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,actual_amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,actualAmountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,output_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,outputAmount}}',
                        rolled_up.last_response_json #>> '{{provider_context,amount_out}}',
                        rolled_up.last_response_json #>> '{{provider_context,amountOut}}',
                        rolled_up.last_response_json #>> '{{provider_context,output_amount}}',
                        rolled_up.last_response_json #>> '{{provider_context,outputAmount}}',
                        rolled_up.last_request_json->>'amount_out',
                        rolled_up.last_request_json->>'amountOut',
                        rolled_up.last_request_json->>'output_amount',
                        rolled_up.last_request_json->>'outputAmount',
                        CASE
                            WHEN leg.leg_type = 'cctp_bridge'
                                THEN rolled_up.last_request_json->>'amount'
                        END,
                        rolled_up.last_request_json #>> '{{price_route,destAmount}}',
                        rolled_up.last_min_amount_out,
                        CASE
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,destination_amount}}') ~ '^[0-9]+$'
                                THEN rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,destination_amount}}'
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,destinationAmount}}') ~ '^[0-9]+$'
                                THEN rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,destinationAmount}}'
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,source_amount}}') ~ '^[0-9]+$'
                                THEN rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,source_amount}}'
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,sourceAmount}}') ~ '^[0-9]+$'
                                THEN rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,sourceAmount}}'
                            WHEN leg.leg_type = 'unit_deposit'
                             AND (rolled_up.last_response_json->>'amount') ~ '^[0-9]+$'
                                THEN rolled_up.last_response_json->>'amount'
                        END,
                        rolled_up.last_response_json #>> '{{observed_state,amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,amountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,output_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,outputAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,amountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,output_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,outputAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,previous_observed_state,output_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,previous_observed_state,outputAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,destination_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,previous_observed_state,destinationAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,amount_out}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,amountOut}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,output_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,outputAmount}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,decoded_message_body,amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,destination_amount}}',
                        rolled_up.last_response_json #>> '{{observed_state,provider_observed_state,destinationAmount}}',
                        CASE
                            WHEN leg.leg_type = 'cctp_bridge'
                                THEN rolled_up.last_amount_in
                        END
                    )
                    ELSE NULL
                END,
                updated_at = now()
            FROM rolled_up
            WHERE leg.id = $1
            RETURNING {EXECUTION_LEG_RETURNING_COLUMNS}
            "#
        ))
        .bind(execution_leg_id)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.refresh_execution_leg_from_actions",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        row.map(|row| self.map_execution_leg_row(&row)).transpose()
    }

    pub async fn update_execution_leg_usd_valuation(
        &self,
        id: Uuid,
        usd_valuation: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionLeg> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_legs leg
            SET
                usd_valuation_json = $2,
                updated_at = $3
            WHERE id = $1
            RETURNING {EXECUTION_LEG_RETURNING_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(usd_valuation)
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.update_execution_leg_usd_valuation",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_execution_leg_row(&row)
    }

    async fn refresh_execution_leg_for_step(
        &self,
        step: &OrderExecutionStep,
    ) -> RouterCoreResult<()> {
        if let Some(execution_leg_id) = step.execution_leg_id {
            let _ = self
                .refresh_execution_leg_from_actions(execution_leg_id)
                .await?;
        }
        Ok(())
    }

    pub async fn supersede_execution_steps_after_index(
        &self,
        execution_attempt_id: Uuid,
        after_step_index: i32,
        reason: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<Vec<OrderExecutionStep>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'superseded',
                error_json = $3,
                completed_at = COALESCE(completed_at, $4),
                updated_at = $4
            WHERE execution_attempt_id = $1
              AND step_index > $2
              AND status IN ('planned', 'ready', 'waiting', 'running')
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(execution_attempt_id)
        .bind(after_step_index)
        .bind(reason)
        .bind(updated_at)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.supersede_execution_steps_after_index",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        let steps = rows
            .iter()
            .map(|row| self.map_execution_step_row(row))
            .collect::<RouterCoreResult<Vec<_>>>()?;
        let mut refreshed_leg_ids = std::collections::BTreeSet::new();
        for step in &steps {
            if let Some(execution_leg_id) = step.execution_leg_id {
                if refreshed_leg_ids.insert(execution_leg_id) {
                    let _ = self
                        .refresh_execution_leg_from_actions(execution_leg_id)
                        .await?;
                }
            }
        }
        Ok(steps)
    }

    pub async fn transition_execution_step_status(
        &self,
        id: Uuid,
        from_status: OrderExecutionStepStatus,
        to_status: OrderExecutionStepStatus,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = $2,
                started_at = CASE
                    WHEN $2 = 'running' THEN COALESCE(started_at, $4)
                    ELSE started_at
                END,
                updated_at = $4
            WHERE id = $1
              AND status = $3
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(to_status.to_db_string())
        .bind(from_status.to_db_string())
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.transition_execution_step_status",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn complete_execution_step(
        &self,
        id: Uuid,
        response: serde_json::Value,
        tx_hash: Option<String>,
        usd_valuation: serde_json::Value,
        completed_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'completed',
                response_json = CASE
                    WHEN response_json = '{{}}'::jsonb THEN $2
                    ELSE response_json || $2
                END,
                tx_hash = COALESCE($3, tx_hash),
                usd_valuation_json = $4,
                completed_at = $5,
                updated_at = $5
            WHERE id = $1
              AND status = 'running'
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(response)
        .bind(tx_hash)
        .bind(usd_valuation)
        .bind(completed_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.complete_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn persist_execution_step_completion(
        &self,
        input: PersistStepCompletionRecord,
    ) -> RouterCoreResult<StepCompletionRecord> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;

            let mut provider_operation_id = None;
            if let Some(operation) = input.operation {
                let operation_id = sqlx_core::query_scalar::query_scalar(
                    r#"
                    WITH upserted AS (
                        INSERT INTO order_provider_operations (
                            id,
                            order_id,
                            execution_attempt_id,
                            execution_step_id,
                            provider,
                            operation_type,
                            provider_ref,
                            status,
                            request_json,
                            response_json,
                            observed_state_json,
                            created_at,
                            updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        ON CONFLICT (execution_step_id) WHERE execution_step_id IS NOT NULL
                        DO UPDATE SET
                            order_id = EXCLUDED.order_id,
                            execution_attempt_id = EXCLUDED.execution_attempt_id,
                            provider = EXCLUDED.provider,
                            operation_type = EXCLUDED.operation_type,
                            provider_ref = EXCLUDED.provider_ref,
                            status = EXCLUDED.status,
                            request_json = EXCLUDED.request_json,
                            response_json = EXCLUDED.response_json,
                            observed_state_json = EXCLUDED.observed_state_json,
                            updated_at = EXCLUDED.updated_at
                        WHERE order_provider_operations.status NOT IN ('completed', 'failed', 'expired')
                          AND (
                            order_provider_operations.order_id IS DISTINCT FROM EXCLUDED.order_id
                            OR order_provider_operations.execution_attempt_id IS DISTINCT FROM EXCLUDED.execution_attempt_id
                            OR order_provider_operations.provider IS DISTINCT FROM EXCLUDED.provider
                            OR order_provider_operations.operation_type IS DISTINCT FROM EXCLUDED.operation_type
                            OR order_provider_operations.provider_ref IS DISTINCT FROM EXCLUDED.provider_ref
                            OR order_provider_operations.status IS DISTINCT FROM EXCLUDED.status
                            OR order_provider_operations.request_json IS DISTINCT FROM EXCLUDED.request_json
                            OR order_provider_operations.response_json IS DISTINCT FROM EXCLUDED.response_json
                            OR order_provider_operations.observed_state_json IS DISTINCT FROM EXCLUDED.observed_state_json
                          )
                        RETURNING id
                    )
                    SELECT id FROM upserted
                    UNION ALL
                    SELECT id
                    FROM order_provider_operations
                    WHERE execution_step_id = $4
                    LIMIT 1
                    "#,
                )
                .bind(operation.id)
                .bind(operation.order_id)
                .bind(operation.execution_attempt_id)
                .bind(operation.execution_step_id)
                .bind(&operation.provider)
                .bind(operation.operation_type.to_db_string())
                .bind(operation.provider_ref.clone())
                .bind(operation.status.to_db_string())
                .bind(operation.request.clone())
                .bind(operation.response.clone())
                .bind(operation.observed_state.clone())
                .bind(operation.created_at)
                .bind(operation.updated_at)
                .fetch_one(&mut *tx)
                .await?;
                provider_operation_id = Some(operation_id);

                for mut address in input.addresses {
                    address.provider_operation_id = provider_operation_id;
                    let query = if address.execution_step_id.is_some() {
                        r#"
                        WITH upserted AS (
                            INSERT INTO order_provider_addresses (
                                id,
                                order_id,
                                execution_step_id,
                                provider_operation_id,
                                provider,
                                role,
                                chain_id,
                                asset_id,
                                address,
                                memo,
                                expires_at,
                                metadata_json,
                                created_at,
                                updated_at
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                            ON CONFLICT (execution_step_id, provider, role, chain_id, address)
                            WHERE execution_step_id IS NOT NULL
                            DO UPDATE SET
                                provider_operation_id = COALESCE(
                                    EXCLUDED.provider_operation_id,
                                    order_provider_addresses.provider_operation_id
                                ),
                                asset_id = EXCLUDED.asset_id,
                                memo = EXCLUDED.memo,
                                expires_at = EXCLUDED.expires_at,
                                metadata_json = EXCLUDED.metadata_json,
                                updated_at = EXCLUDED.updated_at
                            WHERE order_provider_addresses.updated_at <= EXCLUDED.updated_at
                            RETURNING id
                        )
                        SELECT id
                        FROM upserted
                        UNION ALL
                        SELECT id
                        FROM order_provider_addresses
                        WHERE execution_step_id = $3
                          AND provider = $5
                          AND role = $6
                          AND chain_id = $7
                          AND address = $9
                          AND NOT EXISTS (SELECT 1 FROM upserted)
                        LIMIT 1
                        "#
                    } else {
                        r#"
                        WITH upserted AS (
                            INSERT INTO order_provider_addresses (
                                id,
                                order_id,
                                execution_step_id,
                                provider_operation_id,
                                provider,
                                role,
                                chain_id,
                                asset_id,
                                address,
                                memo,
                                expires_at,
                                metadata_json,
                                created_at,
                                updated_at
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                            ON CONFLICT (order_id, provider, role, chain_id, address)
                            WHERE execution_step_id IS NULL
                            DO UPDATE SET
                                provider_operation_id = COALESCE(
                                    EXCLUDED.provider_operation_id,
                                    order_provider_addresses.provider_operation_id
                                ),
                                asset_id = EXCLUDED.asset_id,
                                memo = EXCLUDED.memo,
                                expires_at = EXCLUDED.expires_at,
                                metadata_json = EXCLUDED.metadata_json,
                                updated_at = EXCLUDED.updated_at
                            WHERE order_provider_addresses.updated_at <= EXCLUDED.updated_at
                            RETURNING id
                        )
                        SELECT id
                        FROM upserted
                        UNION ALL
                        SELECT id
                        FROM order_provider_addresses
                        WHERE order_id = $2
                          AND execution_step_id IS NULL
                          AND provider = $5
                          AND role = $6
                          AND chain_id = $7
                          AND address = $9
                          AND NOT EXISTS (SELECT 1 FROM upserted)
                        LIMIT 1
                        "#
                    };
                    let _: Uuid = sqlx_core::query_scalar::query_scalar(query)
                        .bind(address.id)
                        .bind(address.order_id)
                        .bind(address.execution_step_id)
                        .bind(address.provider_operation_id)
                        .bind(&address.provider)
                        .bind(address.role.to_db_string())
                        .bind(address.chain.as_str())
                        .bind(address.asset.as_ref().map(|asset| asset.as_str()))
                        .bind(&address.address)
                        .bind(address.memo.clone())
                        .bind(address.expires_at)
                        .bind(address.metadata.clone())
                        .bind(address.created_at)
                        .bind(address.updated_at)
                        .fetch_one(&mut *tx)
                        .await?;
                }
            }

            let step_row = sqlx_core::query::query(&format!(
                r#"
                WITH completed AS (
                    UPDATE order_execution_steps
                    SET
                        status = 'completed',
                        response_json = CASE
                            WHEN response_json = '{{}}'::jsonb THEN $2
                            ELSE response_json || $2
                        END,
                        tx_hash = COALESCE($3, tx_hash),
                        usd_valuation_json = $4,
                        completed_at = COALESCE(completed_at, $5),
                        updated_at = $5
                    WHERE id = $1
                      AND status = 'running'
                    RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
                )
                SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                FROM completed
                UNION ALL
                SELECT {EXECUTION_STEP_SELECT_COLUMNS}
                FROM order_execution_steps
                WHERE id = $1
                  AND status = 'completed'
                  AND NOT EXISTS (SELECT 1 FROM completed)
                LIMIT 1
                "#
            ))
            .bind(input.step_id)
            .bind(input.response)
            .bind(input.tx_hash)
            .bind(input.usd_valuation)
            .bind(input.completed_at)
            .fetch_one(&mut *tx)
            .await?;
            let step = self.map_execution_step_row(&step_row)?;

            if let Some(execution_leg_id) = step.execution_leg_id {
                let _ = sqlx_core::query::query(
                    r#"
                    UPDATE order_execution_legs
                    SET
                        status = 'completed',
                        actual_amount_in = COALESCE(actual_amount_in, amount_in),
                        actual_amount_out = COALESCE(actual_amount_out, expected_amount_out),
                        completed_at = COALESCE(completed_at, $2),
                        updated_at = $2
                    WHERE id = $1
                      AND status IN ('running', 'completed')
                    "#,
                )
                .bind(execution_leg_id)
                .bind(input.completed_at)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok::<StepCompletionRecord, RouterCoreError>(StepCompletionRecord {
                step,
                provider_operation_id,
            })
        }
        .await;
        telemetry::record_db_query(
            "order.persist_execution_step_completion",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn mark_execution_order_completed(
        &self,
        order_id: Uuid,
        attempt_id: Uuid,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<CompletedExecutionOrder> {
        let started = Instant::now();
        let result = async {
            let mut tx = self.pool.begin().await?;
            let order_row = sqlx_core::query::query(&format!(
                r#"
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                FOR UPDATE OF ro
                "#
            ))
            .bind(order_id)
            .fetch_one(&mut *tx)
            .await?;
            let locked_order = self.map_order_row(&order_row)?;

            if let Some(funding_vault_id) = locked_order.funding_vault_id {
                let updated = sqlx_core::query::query(
                    r#"
                    UPDATE deposit_vaults
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = $4
                    "#,
                )
                .bind(funding_vault_id)
                .bind(DepositVaultStatus::Completed.to_db_string())
                .bind(now)
                .bind(DepositVaultStatus::Executing.to_db_string())
                .execute(&mut *tx)
                .await?;
                if updated.rows_affected() == 0 {
                    let status = sqlx_core::query_scalar::query_scalar::<_, String>(
                        "SELECT status FROM deposit_vaults WHERE id = $1",
                    )
                    .bind(funding_vault_id)
                    .fetch_one(&mut *tx)
                    .await?;
                    let Some(status) = DepositVaultStatus::from_db_string(&status) else {
                        return Err(RouterCoreError::InvalidData {
                            message: format!(
                                "unknown deposit vault status for vault {funding_vault_id}"
                            ),
                        });
                    };
                    if status != DepositVaultStatus::Completed {
                        return Err(RouterCoreError::Conflict {
                            message: format!(
                                "funding vault {} cannot complete from {}",
                                funding_vault_id,
                                status.to_db_string()
                            ),
                        });
                    }
                }
            }

            let order_row = sqlx_core::query::query(&format!(
                r#"
                WITH updated AS (
                    UPDATE router_orders
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = $4
                    RETURNING *
                )
                SELECT {ORDER_SELECT_COLUMNS}
                FROM updated ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                UNION ALL
                SELECT {ORDER_SELECT_COLUMNS}
                FROM router_orders ro
                LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
                LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
                WHERE ro.id = $1
                  AND ro.status = $2
                LIMIT 1
                "#
            ))
            .bind(order_id)
            .bind(RouterOrderStatus::Completed.to_db_string())
            .bind(now)
            .bind(RouterOrderStatus::Executing.to_db_string())
            .fetch_one(&mut *tx)
            .await?;
            let order = self.map_order_row(&order_row)?;

            let attempt_row = sqlx_core::query::query(&format!(
                r#"
                WITH updated AS (
                    UPDATE order_execution_attempts
                    SET status = $2, updated_at = $3
                    WHERE id = $1
                      AND status = $4
                    RETURNING {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                )
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM updated
                UNION ALL
                SELECT {EXECUTION_ATTEMPT_SELECT_COLUMNS}
                FROM order_execution_attempts
                WHERE id = $1
                  AND status = $2
                LIMIT 1
                "#
            ))
            .bind(attempt_id)
            .bind(OrderExecutionAttemptStatus::Completed.to_db_string())
            .bind(now)
            .bind(OrderExecutionAttemptStatus::Active.to_db_string())
            .fetch_one(&mut *tx)
            .await?;
            let attempt = self.map_execution_attempt_row(&attempt_row)?;

            tx.commit().await?;
            Ok::<CompletedExecutionOrder, RouterCoreError>(CompletedExecutionOrder {
                order,
                attempt,
            })
        }
        .await;
        telemetry::record_db_query(
            "order.mark_execution_order_completed",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn complete_wait_for_deposit_step_for_order(
        &self,
        order_id: Uuid,
        response: serde_json::Value,
        tx_hash: Option<String>,
        completed_at: DateTime<Utc>,
    ) -> RouterCoreResult<Option<OrderExecutionStep>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'completed',
                response_json = CASE
                    WHEN response_json = '{{}}'::jsonb THEN $2
                    ELSE response_json
                END,
                tx_hash = COALESCE($3, tx_hash),
                completed_at = COALESCE(completed_at, $4),
                updated_at = $4
            WHERE order_id = $1
              AND step_type = 'wait_for_deposit'
              AND status = 'waiting'
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(order_id)
        .bind(response)
        .bind(tx_hash)
        .bind(completed_at)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.complete_wait_for_deposit_step_for_order",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        row.map(|row| self.map_execution_step_row(&row)).transpose()
    }

    pub async fn wait_execution_step(
        &self,
        id: Uuid,
        response: serde_json::Value,
        tx_hash: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'waiting',
                response_json = $2,
                tx_hash = COALESCE($3, tx_hash),
                next_attempt_at = NULL,
                updated_at = $4
            WHERE id = $1
              AND status = 'running'
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(response)
        .bind(tx_hash)
        .bind(updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.wait_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn complete_observed_execution_step(
        &self,
        id: Uuid,
        response: serde_json::Value,
        tx_hash: Option<String>,
        usd_valuation: serde_json::Value,
        completed_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'completed',
                response_json = CASE
                    WHEN response_json = '{{}}'::jsonb THEN $2
                    ELSE response_json || $2
                END,
                tx_hash = COALESCE($3, tx_hash),
                usd_valuation_json = $4,
                completed_at = $5,
                updated_at = $5
            WHERE id = $1
              AND status IN ('running', 'waiting')
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(response)
        .bind(tx_hash)
        .bind(usd_valuation)
        .bind(completed_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.complete_observed_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn fail_execution_step(
        &self,
        id: Uuid,
        error: serde_json::Value,
        failed_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'failed',
                error_json = $2,
                completed_at = $3,
                updated_at = $3
            WHERE id = $1
              AND status = 'running'
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(error)
        .bind(failed_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.fail_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn fail_unstarted_execution_step(
        &self,
        id: Uuid,
        error: serde_json::Value,
        failed_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'failed',
                error_json = $2,
                completed_at = $3,
                updated_at = $3
            WHERE id = $1
              AND status IN ('planned', 'ready')
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(error)
        .bind(failed_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.fail_unstarted_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    pub async fn fail_observed_execution_step(
        &self,
        id: Uuid,
        error: serde_json::Value,
        failed_at: DateTime<Utc>,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'failed',
                error_json = $2,
                completed_at = $3,
                updated_at = $3
            WHERE id = $1
              AND status IN ('running', 'waiting')
            RETURNING {EXECUTION_STEP_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(error)
        .bind(failed_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.fail_observed_execution_step",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        let step = self.map_execution_step_row(&row)?;
        self.refresh_execution_leg_for_step(&step).await?;
        Ok(step)
    }

    fn map_order_row(&self, row: &sqlx_postgres::PgRow) -> RouterCoreResult<RouterOrder> {
        let order_type = row.get::<String, _>("order_type");
        let order_type = RouterOrderType::from_db_string(&order_type).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported router order type: {order_type}"),
            }
        })?;

        let status = row.get::<String, _>("status");
        let status = RouterOrderStatus::from_db_string(&status).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported router order status: {status}"),
            }
        })?;

        let source_chain = parse_chain_id(row.get::<String, _>("source_chain_id"), "source")?;
        let source_asset = parse_asset_id(row.get::<String, _>("source_asset_id"), "source")?;
        let destination_chain =
            parse_chain_id(row.get::<String, _>("destination_chain_id"), "destination")?;
        let destination_asset =
            parse_asset_id(row.get::<String, _>("destination_asset_id"), "destination")?;

        let action = self.map_order_action_row(order_type, row)?;

        Ok(RouterOrder {
            id: row.get("id"),
            order_type,
            status,
            funding_vault_id: row.get("funding_vault_id"),
            source_asset: DepositAsset {
                chain: source_chain,
                asset: source_asset,
            },
            destination_asset: DepositAsset {
                chain: destination_chain,
                asset: destination_asset,
            },
            recipient_address: row.get("recipient_address"),
            refund_address: row.get("refund_address"),
            action,
            action_timeout_at: row.get("action_timeout_at"),
            idempotency_key: row.get("idempotency_key"),
            workflow_trace_id: row.get("workflow_trace_id"),
            workflow_parent_span_id: row.get("workflow_parent_span_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_order_action_row(
        &self,
        order_type: RouterOrderType,
        row: &sqlx_postgres::PgRow,
    ) -> RouterCoreResult<RouterOrderAction> {
        match order_type {
            RouterOrderType::MarketOrder => {
                let order_kind = row
                    .get::<Option<String>, _>("market_order_kind")
                    .ok_or_else(|| RouterCoreError::InvalidData {
                        message: "market order row is missing market_order_actions".to_string(),
                    })?;
                let order_kind =
                    MarketOrderKindType::from_db_string(&order_kind).ok_or_else(|| {
                        RouterCoreError::InvalidData {
                            message: format!("unsupported market order kind: {order_kind}"),
                        }
                    })?;
                let order_kind = match order_kind {
                    MarketOrderKindType::ExactIn => MarketOrderKind::ExactIn {
                        amount_in: required_action_amount(row, "market_order_amount_in")?,
                        min_amount_out: row.get("market_order_min_amount_out"),
                    },
                    MarketOrderKindType::ExactOut => MarketOrderKind::ExactOut {
                        amount_out: required_action_amount(row, "market_order_amount_out")?,
                        max_amount_in: row.get("market_order_max_amount_in"),
                    },
                };
                let slippage_bps = row
                    .get::<Option<i64>, _>("market_order_slippage_bps")
                    .map(|value| {
                        u64::try_from(value).map_err(|err| RouterCoreError::InvalidData {
                            message: format!("invalid market order slippage_bps: {err}"),
                        })
                    })
                    .transpose()?;

                Ok(RouterOrderAction::MarketOrder(
                    crate::models::MarketOrderAction {
                        order_kind,
                        slippage_bps,
                    },
                ))
            }
            RouterOrderType::LimitOrder => {
                let residual_policy = row
                    .get::<Option<String>, _>("limit_order_residual_policy")
                    .ok_or_else(|| RouterCoreError::InvalidData {
                        message: "limit order row is missing limit_order_actions".to_string(),
                    })?;
                let residual_policy = LimitOrderResidualPolicy::from_db_string(&residual_policy)
                    .ok_or_else(|| RouterCoreError::InvalidData {
                        message: format!(
                            "unsupported limit order residual policy: {residual_policy}"
                        ),
                    })?;

                Ok(RouterOrderAction::LimitOrder(LimitOrderAction {
                    input_amount: required_action_amount(row, "limit_order_input_amount")?,
                    output_amount: required_action_amount(row, "limit_order_output_amount")?,
                    residual_policy,
                }))
            }
        }
    }

    fn map_quote_row(&self, row: &sqlx_postgres::PgRow) -> RouterCoreResult<MarketOrderQuote> {
        let order_kind = row.get::<String, _>("order_kind");
        let order_kind = MarketOrderKindType::from_db_string(&order_kind).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported market order kind: {order_kind}"),
            }
        })?;
        let source_chain = parse_chain_id(row.get::<String, _>("source_chain_id"), "quote source")?;
        let source_asset = parse_asset_id(row.get::<String, _>("source_asset_id"), "quote source")?;
        let destination_chain = parse_chain_id(
            row.get::<String, _>("destination_chain_id"),
            "quote destination",
        )?;
        let destination_asset = parse_asset_id(
            row.get::<String, _>("destination_asset_id"),
            "quote destination",
        )?;

        Ok(MarketOrderQuote {
            id: row.get("id"),
            order_id: row.get("order_id"),
            source_asset: DepositAsset {
                chain: source_chain,
                asset: source_asset,
            },
            destination_asset: DepositAsset {
                chain: destination_chain,
                asset: destination_asset,
            },
            recipient_address: row.get("recipient_address"),
            provider_id: row.get("provider_id"),
            order_kind,
            amount_in: row.get("amount_in"),
            amount_out: row.get("amount_out"),
            min_amount_out: row.get("min_amount_out"),
            max_amount_in: row.get("max_amount_in"),
            slippage_bps: row
                .get::<Option<i64>, _>("slippage_bps")
                .map(|value| {
                    u64::try_from(value).map_err(|err| RouterCoreError::InvalidData {
                        message: format!("invalid quote slippage_bps: {err}"),
                    })
                })
                .transpose()?,
            provider_quote: row.get("provider_quote"),
            usd_valuation: row.get("usd_valuation_json"),
            expires_at: row.get("expires_at"),
            created_at: row.get("created_at"),
        })
    }

    fn map_limit_quote_row(&self, row: &sqlx_postgres::PgRow) -> RouterCoreResult<LimitOrderQuote> {
        let source_chain = parse_chain_id(row.get::<String, _>("source_chain_id"), "quote source")?;
        let source_asset = parse_asset_id(row.get::<String, _>("source_asset_id"), "quote source")?;
        let destination_chain = parse_chain_id(
            row.get::<String, _>("destination_chain_id"),
            "quote destination",
        )?;
        let destination_asset = parse_asset_id(
            row.get::<String, _>("destination_asset_id"),
            "quote destination",
        )?;
        let residual_policy = row.get::<String, _>("residual_policy");
        let residual_policy = LimitOrderResidualPolicy::from_db_string(&residual_policy)
            .ok_or_else(|| RouterCoreError::InvalidData {
                message: format!("unsupported limit quote residual policy: {residual_policy}"),
            })?;

        Ok(LimitOrderQuote {
            id: row.get("id"),
            order_id: row.get("order_id"),
            source_asset: DepositAsset {
                chain: source_chain,
                asset: source_asset,
            },
            destination_asset: DepositAsset {
                chain: destination_chain,
                asset: destination_asset,
            },
            recipient_address: row.get("recipient_address"),
            provider_id: row.get("provider_id"),
            input_amount: row.get("input_amount"),
            output_amount: row.get("output_amount"),
            residual_policy,
            provider_quote: row.get("provider_quote"),
            usd_valuation: row.get("usd_valuation_json"),
            expires_at: row.get("expires_at"),
            created_at: row.get("created_at"),
        })
    }

    fn map_custody_vault_row(&self, row: &sqlx_postgres::PgRow) -> RouterCoreResult<CustodyVault> {
        let role = row.get::<String, _>("role");
        let role = CustodyVaultRole::from_db_string(&role).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported custody vault role: {role}"),
            }
        })?;
        let visibility = row.get::<String, _>("visibility");
        let visibility = CustodyVaultVisibility::from_db_string(&visibility).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported custody vault visibility: {visibility}"),
            }
        })?;
        let control_type = row.get::<String, _>("control_type");
        let control_type =
            CustodyVaultControlType::from_db_string(&control_type).ok_or_else(|| {
                RouterCoreError::InvalidData {
                    message: format!("unsupported custody vault control type: {control_type}"),
                }
            })?;
        let status = row.get::<String, _>("status");
        let status = CustodyVaultStatus::from_db_string(&status).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported custody vault status: {status}"),
            }
        })?;
        let chain = parse_chain_id(row.get::<String, _>("chain_id"), "custody vault")?;
        let asset = row
            .get::<Option<String>, _>("asset_id")
            .map(|asset| parse_asset_id(asset, "custody vault"))
            .transpose()?;
        let derivation_salt = row
            .get::<Option<Vec<u8>>, _>("derivation_salt")
            .map(|salt| {
                salt.try_into().map_err(|_| RouterCoreError::InvalidData {
                    message: "custody vault derivation_salt must be 32 bytes".to_string(),
                })
            })
            .transpose()?;

        Ok(CustodyVault {
            id: row.get("id"),
            order_id: row.get("order_id"),
            role,
            visibility,
            chain,
            asset,
            address: row.get("address"),
            control_type,
            derivation_salt,
            signer_ref: row.get("signer_ref"),
            status,
            metadata: row.get("metadata_json"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_provider_operation_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterCoreResult<OrderProviderOperation> {
        let operation_type = row.get::<String, _>("operation_type");
        let operation_type =
            ProviderOperationType::from_db_string(&operation_type).ok_or_else(|| {
                RouterCoreError::InvalidData {
                    message: format!("unsupported provider operation type: {operation_type}"),
                }
            })?;
        let status = row.get::<String, _>("status");
        let status = ProviderOperationStatus::from_db_string(&status).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported provider operation status: {status}"),
            }
        })?;

        Ok(OrderProviderOperation {
            id: row.get("id"),
            order_id: row.get("order_id"),
            execution_attempt_id: row.get("execution_attempt_id"),
            execution_step_id: row.get("execution_step_id"),
            provider: row.get("provider"),
            operation_type,
            provider_ref: row.get("provider_ref"),
            status,
            request: row.get("request_json"),
            response: row.get("response_json"),
            observed_state: row.get("observed_state_json"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_provider_operation_hint_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterCoreResult<OrderProviderOperationHint> {
        let hint_kind = row.get::<String, _>("hint_kind");
        let hint_kind = ProviderOperationHintKind::from_db_string(&hint_kind).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported provider operation hint kind: {hint_kind}"),
            }
        })?;
        let status = row.get::<String, _>("status");
        let status = ProviderOperationHintStatus::from_db_string(&status).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported provider operation hint status: {status}"),
            }
        })?;

        Ok(OrderProviderOperationHint {
            id: row.get("id"),
            provider_operation_id: row.get("provider_operation_id"),
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

    fn map_provider_address_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterCoreResult<OrderProviderAddress> {
        let role = row.get::<String, _>("role");
        let role = ProviderAddressRole::from_db_string(&role).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported provider address role: {role}"),
            }
        })?;
        let chain = parse_chain_id(row.get::<String, _>("chain_id"), "provider address")?;
        let asset = row
            .get::<Option<String>, _>("asset_id")
            .map(|asset| parse_asset_id(asset, "provider address"))
            .transpose()?;

        Ok(OrderProviderAddress {
            id: row.get("id"),
            order_id: row.get("order_id"),
            execution_step_id: row.get("execution_step_id"),
            provider_operation_id: row.get("provider_operation_id"),
            provider: row.get("provider"),
            role,
            chain,
            asset,
            address: row.get("address"),
            memo: row.get("memo"),
            expires_at: row.get("expires_at"),
            metadata: row.get("metadata_json"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_execution_step_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterCoreResult<OrderExecutionStep> {
        let step_type = row.get::<String, _>("step_type");
        let step_type = OrderExecutionStepType::from_db_string(&step_type).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported order execution step type: {step_type}"),
            }
        })?;
        let status = row.get::<String, _>("status");
        let status = OrderExecutionStepStatus::from_db_string(&status).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported order execution step status: {status}"),
            }
        })?;

        Ok(OrderExecutionStep {
            id: row.get("id"),
            order_id: row.get("order_id"),
            execution_attempt_id: row.get("execution_attempt_id"),
            execution_leg_id: row.get("execution_leg_id"),
            transition_decl_id: row.get("transition_decl_id"),
            step_index: row.get("step_index"),
            step_type,
            provider: row.get("provider"),
            status,
            input_asset: parse_optional_deposit_asset(
                row.get("input_chain_id"),
                row.get("input_asset_id"),
                "input",
            )?,
            output_asset: parse_optional_deposit_asset(
                row.get("output_chain_id"),
                row.get("output_asset_id"),
                "output",
            )?,
            amount_in: row.get("amount_in"),
            min_amount_out: row.get("min_amount_out"),
            tx_hash: row.get("tx_hash"),
            provider_ref: row.get("provider_ref"),
            idempotency_key: row.get("idempotency_key"),
            attempt_count: row.get("attempt_count"),
            next_attempt_at: row.get("next_attempt_at"),
            started_at: row.get("started_at"),
            completed_at: row.get("completed_at"),
            details: row.get("details_json"),
            request: row.get("request_json"),
            response: row.get("response_json"),
            error: row.get("error_json"),
            usd_valuation: row.get("usd_valuation_json"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_execution_leg_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterCoreResult<OrderExecutionLeg> {
        let status = row.get::<String, _>("status");
        let status = OrderExecutionStepStatus::from_db_string(&status).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported order execution leg status: {status}"),
            }
        })?;
        let input_chain = parse_chain_id(row.get::<String, _>("input_chain_id"), "leg input")?;
        let input_asset = parse_asset_id(row.get::<String, _>("input_asset_id"), "leg input")?;
        let output_chain = parse_chain_id(row.get::<String, _>("output_chain_id"), "leg output")?;
        let output_asset = parse_asset_id(row.get::<String, _>("output_asset_id"), "leg output")?;

        Ok(OrderExecutionLeg {
            id: row.get("id"),
            order_id: row.get("order_id"),
            execution_attempt_id: row.get("execution_attempt_id"),
            transition_decl_id: row.get("transition_decl_id"),
            leg_index: row.get("leg_index"),
            leg_type: row.get("leg_type"),
            provider: row.get("provider"),
            status,
            input_asset: DepositAsset {
                chain: input_chain,
                asset: input_asset,
            },
            output_asset: DepositAsset {
                chain: output_chain,
                asset: output_asset,
            },
            amount_in: row.get("amount_in"),
            expected_amount_out: row.get("expected_amount_out"),
            min_amount_out: row.get("min_amount_out"),
            actual_amount_in: row.get("actual_amount_in"),
            actual_amount_out: row.get("actual_amount_out"),
            started_at: row.get("started_at"),
            completed_at: row.get("completed_at"),
            provider_quote_expires_at: row.get("provider_quote_expires_at"),
            details: row.get("details_json"),
            usd_valuation: row.get("usd_valuation_json"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_execution_attempt_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterCoreResult<OrderExecutionAttempt> {
        let attempt_kind = row.get::<String, _>("attempt_kind");
        let attempt_kind =
            OrderExecutionAttemptKind::from_db_string(&attempt_kind).ok_or_else(|| {
                RouterCoreError::InvalidData {
                    message: format!("unsupported order execution attempt kind: {attempt_kind}"),
                }
            })?;
        let status = row.get::<String, _>("status");
        let status = OrderExecutionAttemptStatus::from_db_string(&status).ok_or_else(|| {
            RouterCoreError::InvalidData {
                message: format!("unsupported order execution attempt status: {status}"),
            }
        })?;

        Ok(OrderExecutionAttempt {
            id: row.get("id"),
            order_id: row.get("order_id"),
            attempt_index: row.get("attempt_index"),
            attempt_kind,
            status,
            trigger_step_id: row.get("trigger_step_id"),
            trigger_provider_operation_id: row.get("trigger_provider_operation_id"),
            failure_reason: row.get("failure_reason_json"),
            input_custody_snapshot: row.get("input_custody_snapshot_json"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }
}

async fn insert_execution_leg_tx(
    tx: &mut sqlx_core::transaction::Transaction<'_, Postgres>,
    leg: &OrderExecutionLeg,
) -> RouterCoreResult<()> {
    sqlx_core::query::query(
        r#"
        INSERT INTO order_execution_legs (
            id,
            order_id,
            execution_attempt_id,
            transition_decl_id,
            leg_index,
            leg_type,
            provider,
            status,
            input_chain_id,
            input_asset_id,
            output_chain_id,
            output_asset_id,
            amount_in,
            expected_amount_out,
            min_amount_out,
            actual_amount_in,
            actual_amount_out,
            started_at,
            completed_at,
            provider_quote_expires_at,
            details_json,
            usd_valuation_json,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
        )
        "#,
    )
    .bind(leg.id)
    .bind(leg.order_id)
    .bind(leg.execution_attempt_id)
    .bind(leg.transition_decl_id.clone())
    .bind(leg.leg_index)
    .bind(&leg.leg_type)
    .bind(&leg.provider)
    .bind(leg.status.to_db_string())
    .bind(leg.input_asset.chain.as_str())
    .bind(leg.input_asset.asset.as_str())
    .bind(leg.output_asset.chain.as_str())
    .bind(leg.output_asset.asset.as_str())
    .bind(&leg.amount_in)
    .bind(&leg.expected_amount_out)
    .bind(leg.min_amount_out.clone())
    .bind(leg.actual_amount_in.clone())
    .bind(leg.actual_amount_out.clone())
    .bind(leg.started_at)
    .bind(leg.completed_at)
    .bind(leg.provider_quote_expires_at)
    .bind(leg.details.clone())
    .bind(leg.usd_valuation.clone())
    .bind(leg.created_at)
    .bind(leg.updated_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_execution_step_tx(
    tx: &mut sqlx_core::transaction::Transaction<'_, Postgres>,
    step: &OrderExecutionStep,
) -> RouterCoreResult<()> {
    sqlx_core::query::query(
        r#"
        INSERT INTO order_execution_steps (
            id,
            order_id,
            execution_attempt_id,
            execution_leg_id,
            transition_decl_id,
            step_index,
            step_type,
            provider,
            status,
            input_chain_id,
            input_asset_id,
            output_chain_id,
            output_asset_id,
            amount_in,
            min_amount_out,
            tx_hash,
            provider_ref,
            idempotency_key,
            attempt_count,
            next_attempt_at,
            started_at,
            completed_at,
            details_json,
            request_json,
            response_json,
            error_json,
            usd_valuation_json,
            created_at,
            updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
            $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
            $23, $24, $25, $26, $27, $28, $29
        )
        "#,
    )
    .bind(step.id)
    .bind(step.order_id)
    .bind(step.execution_attempt_id)
    .bind(step.execution_leg_id)
    .bind(step.transition_decl_id.clone())
    .bind(step.step_index)
    .bind(step.step_type.to_db_string())
    .bind(&step.provider)
    .bind(step.status.to_db_string())
    .bind(step.input_asset.as_ref().map(|asset| asset.chain.as_str()))
    .bind(step.input_asset.as_ref().map(|asset| asset.asset.as_str()))
    .bind(step.output_asset.as_ref().map(|asset| asset.chain.as_str()))
    .bind(step.output_asset.as_ref().map(|asset| asset.asset.as_str()))
    .bind(step.amount_in.clone())
    .bind(step.min_amount_out.clone())
    .bind(step.tx_hash.clone())
    .bind(step.provider_ref.clone())
    .bind(step.idempotency_key.clone())
    .bind(step.attempt_count)
    .bind(step.next_attempt_at)
    .bind(step.started_at)
    .bind(step.completed_at)
    .bind(step.details.clone())
    .bind(step.request.clone())
    .bind(step.response.clone())
    .bind(step.error.clone())
    .bind(step.usd_valuation.clone())
    .bind(step.created_at)
    .bind(step.updated_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn set_json_value(target: &mut serde_json::Value, key: &str, value: serde_json::Value) {
    if let Some(object) = target.as_object_mut() {
        object.insert(key.to_string(), value);
    }
}

fn ensure_execution_leg_plan_matches(
    existing: &OrderExecutionLeg,
    planned: &OrderExecutionLeg,
) -> RouterCoreResult<()> {
    let mut mismatches = Vec::new();
    if existing.order_id != planned.order_id {
        mismatches.push("order_id");
    }
    if existing.execution_attempt_id != planned.execution_attempt_id {
        mismatches.push("execution_attempt_id");
    }
    if existing.transition_decl_id != planned.transition_decl_id {
        mismatches.push("transition_decl_id");
    }
    if existing.leg_index != planned.leg_index {
        mismatches.push("leg_index");
    }
    if existing.leg_type != planned.leg_type {
        mismatches.push("leg_type");
    }
    if existing.provider != planned.provider {
        mismatches.push("provider");
    }
    if existing.input_asset != planned.input_asset {
        mismatches.push("input_asset");
    }
    if existing.output_asset != planned.output_asset {
        mismatches.push("output_asset");
    }
    if existing.amount_in != planned.amount_in {
        mismatches.push("amount_in");
    }
    if existing.expected_amount_out != planned.expected_amount_out {
        mismatches.push("expected_amount_out");
    }
    if existing.min_amount_out != planned.min_amount_out {
        mismatches.push("min_amount_out");
    }
    if !same_timestamptz_at_db_precision(
        existing.provider_quote_expires_at,
        planned.provider_quote_expires_at,
    ) {
        mismatches.push("provider_quote_expires_at");
    }
    if existing.details != planned.details {
        mismatches.push("details");
    }

    if mismatches.is_empty() {
        return Ok(());
    }

    Err(RouterCoreError::InvalidData {
        message: format!(
            "execution leg materialization drift for order {} leg_index {}: {}",
            planned.order_id,
            planned.leg_index,
            mismatches.join(", ")
        ),
    })
}

fn same_timestamptz_at_db_precision(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.timestamp_micros() == right.timestamp_micros(),
        (None, None) => true,
        _ => false,
    }
}

fn ensure_execution_step_plan_matches(
    existing: &OrderExecutionStep,
    planned: &OrderExecutionStep,
) -> RouterCoreResult<()> {
    let mut mismatches = Vec::new();
    if existing.order_id != planned.order_id {
        mismatches.push("order_id");
    }
    if existing.execution_attempt_id != planned.execution_attempt_id {
        mismatches.push("execution_attempt_id");
    }
    if existing.execution_leg_id != planned.execution_leg_id {
        mismatches.push("execution_leg_id");
    }
    if existing.transition_decl_id != planned.transition_decl_id {
        mismatches.push("transition_decl_id");
    }
    if existing.step_index != planned.step_index {
        mismatches.push("step_index");
    }
    if existing.step_type != planned.step_type {
        mismatches.push("step_type");
    }
    if existing.provider != planned.provider {
        mismatches.push("provider");
    }
    if existing.input_asset != planned.input_asset {
        mismatches.push("input_asset");
    }
    if existing.output_asset != planned.output_asset {
        mismatches.push("output_asset");
    }
    if existing.amount_in != planned.amount_in {
        mismatches.push("amount_in");
    }
    if existing.min_amount_out != planned.min_amount_out {
        mismatches.push("min_amount_out");
    }
    if existing.provider_ref != planned.provider_ref {
        mismatches.push("provider_ref");
    }
    if existing.details != planned.details {
        mismatches.push("details");
    }
    if existing.request != planned.request {
        mismatches.push("request");
    }

    if mismatches.is_empty() {
        return Ok(());
    }

    Err(RouterCoreError::InvalidData {
        message: format!(
            "execution step materialization drift for order {} step_index {}: {}",
            planned.order_id,
            planned.step_index,
            mismatches.join(", ")
        ),
    })
}

fn parse_chain_id(value: String, label: &str) -> RouterCoreResult<ChainId> {
    ChainId::parse(value).map_err(|err| RouterCoreError::InvalidData {
        message: format!("unsupported {label} chain id: {err}"),
    })
}

fn parse_asset_id(value: String, label: &str) -> RouterCoreResult<AssetId> {
    AssetId::parse(value).map_err(|err| RouterCoreError::InvalidData {
        message: format!("unsupported {label} asset id: {err}"),
    })
}

fn parse_optional_deposit_asset(
    chain: Option<String>,
    asset: Option<String>,
    label: &str,
) -> RouterCoreResult<Option<DepositAsset>> {
    match (chain, asset) {
        (Some(chain), Some(asset)) => Ok(Some(DepositAsset {
            chain: parse_chain_id(chain, label)?,
            asset: parse_asset_id(asset, label)?,
        })),
        (None, None) => Ok(None),
        _ => Err(RouterCoreError::InvalidData {
            message: format!("{label} execution asset is partially populated"),
        }),
    }
}

struct MarketOrderActionFields {
    order_kind: MarketOrderKindType,
    amount_in: Option<String>,
    min_amount_out: Option<String>,
    amount_out: Option<String>,
    max_amount_in: Option<String>,
    slippage_bps: Option<u64>,
}

fn market_order_action_fields(
    action: &RouterOrderAction,
) -> RouterCoreResult<MarketOrderActionFields> {
    match action {
        RouterOrderAction::MarketOrder(action) => match &action.order_kind {
            MarketOrderKind::ExactIn {
                amount_in,
                min_amount_out,
            } => Ok(MarketOrderActionFields {
                order_kind: MarketOrderKindType::ExactIn,
                amount_in: Some(amount_in.clone()),
                min_amount_out: min_amount_out.clone(),
                amount_out: None,
                max_amount_in: None,
                slippage_bps: action.slippage_bps,
            }),
            MarketOrderKind::ExactOut {
                amount_out,
                max_amount_in,
            } => Ok(MarketOrderActionFields {
                order_kind: MarketOrderKindType::ExactOut,
                amount_in: None,
                min_amount_out: None,
                amount_out: Some(amount_out.clone()),
                max_amount_in: max_amount_in.clone(),
                slippage_bps: action.slippage_bps,
            }),
        },
        RouterOrderAction::LimitOrder(_) => Err(RouterCoreError::InvalidData {
            message: "expected market order action".to_string(),
        }),
    }
}

fn optional_slippage_bps_i64(
    slippage_bps: Option<u64>,
    owner: &str,
) -> RouterCoreResult<Option<i64>> {
    slippage_bps
        .map(|value| {
            i64::try_from(value).map_err(|err| RouterCoreError::Validation {
                message: format!("{owner} slippage_bps does not fit i64: {err}"),
            })
        })
        .transpose()
}

fn limit_order_action_fields(action: &RouterOrderAction) -> RouterCoreResult<LimitOrderAction> {
    match action {
        RouterOrderAction::LimitOrder(action) => Ok(action.clone()),
        RouterOrderAction::MarketOrder(_) => Err(RouterCoreError::InvalidData {
            message: "expected limit order action".to_string(),
        }),
    }
}

fn required_action_amount(
    row: &sqlx_postgres::PgRow,
    column: &'static str,
) -> RouterCoreResult<String> {
    row.get::<Option<String>, _>(column)
        .ok_or_else(|| RouterCoreError::InvalidData {
            message: format!("order action is missing required {column}"),
        })
}
