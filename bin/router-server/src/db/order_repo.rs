use crate::{
    error::{RouterServerError, RouterServerResult},
    models::{
        CustodyVault, CustodyVaultControlType, CustodyVaultRole, CustodyVaultStatus,
        CustodyVaultVisibility, LimitOrderAction, LimitOrderQuote, LimitOrderResidualPolicy,
        MarketOrderKind, MarketOrderKindType, MarketOrderQuote, OrderExecutionAttempt,
        OrderExecutionAttemptKind, OrderExecutionAttemptStatus, OrderExecutionLeg,
        OrderExecutionStep, OrderExecutionStepStatus, OrderExecutionStepType, OrderProviderAddress,
        OrderProviderOperation, OrderProviderOperationHint, ProviderAddressRole,
        ProviderOperationHintKind, ProviderOperationHintStatus, ProviderOperationStatus,
        ProviderOperationType, RouterOrder, RouterOrderAction, RouterOrderQuote, RouterOrderStatus,
        RouterOrderType,
    },
    protocol::{AssetId, ChainId, DepositAsset},
    telemetry,
};
use chrono::{DateTime, Utc};
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use std::time::Instant;
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
    created_at,
    updated_at
"#;

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
    ) -> RouterServerResult<()> {
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
        .bind(
            i64::try_from(quote.slippage_bps).map_err(|err| RouterServerError::Validation {
                message: format!("quote slippage_bps does not fit i64: {err}"),
            })?,
        )
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

    pub async fn create_limit_order_quote(
        &self,
        quote: &LimitOrderQuote,
    ) -> RouterServerResult<()> {
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
                expires_at,
                created_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14
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
    ) -> RouterServerResult<MarketOrderQuote> {
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
            .bind(i64::try_from(market_action.slippage_bps).map_err(|err| {
                RouterServerError::Validation {
                    message: format!("order slippage_bps does not fit i64: {err}"),
                }
            })?)
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
                RETURNING {QUOTE_SELECT_COLUMNS}
                "#
            ))
            .bind(quote_id)
            .bind(order.id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(RouterServerError::Validation {
                message: format!("quote {quote_id} is no longer orderable"),
            })?;

            let quote = self.map_quote_row(&row)?;
            tx.commit().await?;
            Ok::<MarketOrderQuote, RouterServerError>(quote)
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
    ) -> RouterServerResult<LimitOrderQuote> {
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
                RETURNING {LIMIT_QUOTE_SELECT_COLUMNS}
                "#
            ))
            .bind(quote_id)
            .bind(order.id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(RouterServerError::Validation {
                message: format!("quote {quote_id} is no longer orderable"),
            })?;

            let quote = self.map_limit_quote_row(&row)?;
            tx.commit().await?;
            Ok::<LimitOrderQuote, RouterServerError>(quote)
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
    ) -> RouterServerResult<()> {
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
                return Err(RouterServerError::Validation {
                    message: format!(
                        "order {order_id} could not be rolled back for quote {quote_id}"
                    ),
                });
            }

            tx.commit().await?;
            Ok::<(), RouterServerError>(())
        }
        .await;
        telemetry::record_db_query(
            "order.release_quote_and_delete_quoted_order",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn get(&self, id: Uuid) -> RouterServerResult<RouterOrder> {
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

    pub async fn transition_status(
        &self,
        id: Uuid,
        from_status: RouterOrderStatus,
        to_status: RouterOrderStatus,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<RouterOrder> {
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

    pub async fn get_market_orders_needing_execution_plan(
        &self,
        limit: i64,
    ) -> RouterServerResult<Vec<RouterOrder>> {
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
    ) -> RouterServerResult<Vec<(Uuid, Option<Uuid>)>> {
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
    ) -> RouterServerResult<Vec<RouterOrder>> {
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
    ) -> RouterServerResult<Vec<RouterOrder>> {
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
    ) -> RouterServerResult<MarketOrderQuote> {
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
    ) -> RouterServerResult<MarketOrderQuote> {
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

    pub async fn get_limit_order_quote(
        &self,
        order_id: Uuid,
    ) -> RouterServerResult<LimitOrderQuote> {
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
    ) -> RouterServerResult<LimitOrderQuote> {
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
    ) -> RouterServerResult<RouterOrderQuote> {
        match self.get_market_order_quote_by_id(quote_id).await {
            Ok(quote) => Ok(quote.into()),
            Err(RouterServerError::NotFound) => self
                .get_limit_order_quote_by_id(quote_id)
                .await
                .map(Into::into),
            Err(err) => Err(err),
        }
    }

    pub async fn get_router_order_quote(
        &self,
        order_id: Uuid,
    ) -> RouterServerResult<RouterOrderQuote> {
        match self.get_market_order_quote(order_id).await {
            Ok(quote) => Ok(quote.into()),
            Err(RouterServerError::NotFound) => {
                self.get_limit_order_quote(order_id).await.map(Into::into)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn delete_expired_unassociated_market_order_quotes(
        &self,
        now: DateTime<Utc>,
    ) -> RouterServerResult<u64> {
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
    ) -> RouterServerResult<u64> {
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
    ) -> RouterServerResult<()> {
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
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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

    pub async fn get_execution_attempt(
        &self,
        id: Uuid,
    ) -> RouterServerResult<OrderExecutionAttempt> {
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
    ) -> RouterServerResult<Vec<OrderExecutionAttempt>> {
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
    ) -> RouterServerResult<Option<OrderExecutionAttempt>> {
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
    ) -> RouterServerResult<Option<OrderExecutionAttempt>> {
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
    ) -> RouterServerResult<OrderExecutionAttempt> {
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
    ) -> RouterServerResult<OrderExecutionAttempt> {
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
    ) -> RouterServerResult<OrderExecutionAttempt> {
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
    ) -> RouterServerResult<Vec<RouterOrder>> {
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
    ) -> RouterServerResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT DISTINCT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            JOIN deposit_vaults dv ON dv.id = ro.funding_vault_id
            JOIN order_execution_attempts oea
              ON oea.order_id = ro.id
             AND oea.attempt_kind = 'refund_recovery'
             AND oea.status = 'active'
            WHERE ro.status = 'refunding'
              AND dv.status = 'refunded'
              AND NOT EXISTS (
                  SELECT 1
                  FROM order_execution_steps oes
                  WHERE oes.execution_attempt_id = oea.id
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
    ) -> RouterServerResult<Vec<RouterOrder>> {
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

    pub async fn find_orders_pending_retry_or_refund_decision(
        &self,
        limit: i64,
    ) -> RouterServerResult<Vec<RouterOrder>> {
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

    pub async fn create_custody_vault(&self, vault: &CustodyVault) -> RouterServerResult<()> {
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

    pub async fn get_custody_vaults(
        &self,
        order_id: Uuid,
    ) -> RouterServerResult<Vec<CustodyVault>> {
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

    pub async fn get_custody_vault(&self, id: Uuid) -> RouterServerResult<CustodyVault> {
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
    ) -> RouterServerResult<CustodyVault> {
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

    pub async fn finalize_internal_custody_vaults(
        &self,
        order_id: Uuid,
        status: CustodyVaultStatus,
        metadata_patch: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<Vec<CustodyVault>> {
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
    ) -> RouterServerResult<Vec<RouterOrder>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ORDER_SELECT_COLUMNS}
            FROM router_orders ro
            LEFT JOIN market_order_actions moa ON moa.order_id = ro.id
            LEFT JOIN limit_order_actions loa ON loa.order_id = ro.id
            WHERE ro.status IN (
                'completed',
                'refund_required',
                'refunding',
                'refunded',
                'failed',
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
    ) -> RouterServerResult<Vec<CustodyVault>> {
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
    ) -> RouterServerResult<()> {
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
    ) -> RouterServerResult<Uuid> {
        let started = Instant::now();
        let result = sqlx_core::query_scalar::query_scalar(
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
            RETURNING id
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
    ) -> RouterServerResult<Vec<OrderProviderOperation>> {
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
    ) -> RouterServerResult<OrderProviderOperation> {
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
    ) -> RouterServerResult<Vec<OrderProviderOperation>> {
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

    pub async fn get_provider_operation_by_ref(
        &self,
        provider: &str,
        provider_ref: &str,
    ) -> RouterServerResult<OrderProviderOperation> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {PROVIDER_OPERATION_SELECT_COLUMNS}
            FROM order_provider_operations
            WHERE provider = $1
              AND provider_ref = $2
            ORDER BY updated_at DESC, created_at DESC, id DESC
            LIMIT 1
            "#
        ))
        .bind(provider)
        .bind(provider_ref)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query(
            "order.get_provider_operation_by_ref",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        self.map_provider_operation_row(&row)
    }

    pub async fn update_provider_operation_status(
        &self,
        id: Uuid,
        status: ProviderOperationStatus,
        observed_state: serde_json::Value,
        response: Option<serde_json::Value>,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<OrderProviderOperation> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_provider_operations
            SET
                status = $2,
                observed_state_json = $3,
                response_json = COALESCE($4, response_json),
                updated_at = $5
            WHERE id = $1
            RETURNING {PROVIDER_OPERATION_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(status.to_db_string())
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

        self.map_provider_operation_row(&row)
    }

    pub async fn create_provider_operation_hint(
        &self,
        hint: &OrderProviderOperationHint,
    ) -> RouterServerResult<OrderProviderOperationHint> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
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
                evidence_json = CASE
                    WHEN order_provider_operation_hints.source = 'sauron_provider_operation'
                         AND order_provider_operation_hints.status IN ('processed', 'ignored', 'failed')
                    THEN EXCLUDED.evidence_json
                    ELSE order_provider_operation_hints.evidence_json
                END,
                status = CASE
                    WHEN order_provider_operation_hints.source = 'sauron_provider_operation'
                         AND order_provider_operation_hints.status IN ('processed', 'ignored', 'failed')
                    THEN EXCLUDED.status
                    ELSE order_provider_operation_hints.status
                END,
                error_json = CASE
                    WHEN order_provider_operation_hints.source = 'sauron_provider_operation'
                         AND order_provider_operation_hints.status IN ('processed', 'ignored', 'failed')
                    THEN '{{}}'::jsonb
                    ELSE order_provider_operation_hints.error_json
                END,
                claimed_at = CASE
                    WHEN order_provider_operation_hints.source = 'sauron_provider_operation'
                         AND order_provider_operation_hints.status IN ('processed', 'ignored', 'failed')
                    THEN NULL
                    ELSE order_provider_operation_hints.claimed_at
                END,
                processed_at = CASE
                    WHEN order_provider_operation_hints.source = 'sauron_provider_operation'
                         AND order_provider_operation_hints.status IN ('processed', 'ignored', 'failed')
                    THEN NULL
                    ELSE order_provider_operation_hints.processed_at
                END,
                updated_at = CASE
                    WHEN order_provider_operation_hints.source = 'sauron_provider_operation'
                         AND order_provider_operation_hints.status IN ('processed', 'ignored', 'failed')
                    THEN EXCLUDED.updated_at
                    ELSE order_provider_operation_hints.updated_at
                END
            RETURNING {PROVIDER_OPERATION_HINT_SELECT_COLUMNS}
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
    ) -> RouterServerResult<Vec<OrderProviderOperationHint>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH candidates AS (
                SELECT id
                FROM order_provider_operation_hints
                WHERE status = 'pending'
                   OR (status = 'processing' AND claimed_at < $2 - INTERVAL '5 minutes')
                ORDER BY created_at ASC, id ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
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
        status: ProviderOperationHintStatus,
        error: serde_json::Value,
        now: DateTime<Utc>,
    ) -> RouterServerResult<OrderProviderOperationHint> {
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
            RETURNING {PROVIDER_OPERATION_HINT_SELECT_COLUMNS}
            "#
        ))
        .bind(id)
        .bind(status.to_db_string())
        .bind(error)
        .bind(now)
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
    ) -> RouterServerResult<()> {
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
    ) -> RouterServerResult<Uuid> {
        let started = Instant::now();
        let result = sqlx_core::query_scalar::query_scalar(
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
            ON CONFLICT (order_id, provider, role, chain_id, address)
            DO UPDATE SET
                execution_step_id = EXCLUDED.execution_step_id,
                provider_operation_id = COALESCE(
                    EXCLUDED.provider_operation_id,
                    order_provider_addresses.provider_operation_id
                ),
                asset_id = EXCLUDED.asset_id,
                memo = EXCLUDED.memo,
                expires_at = EXCLUDED.expires_at,
                metadata_json = EXCLUDED.metadata_json,
                updated_at = EXCLUDED.updated_at
            RETURNING id
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
    ) -> RouterServerResult<Vec<OrderProviderAddress>> {
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
    ) -> RouterServerResult<Vec<OrderProviderAddress>> {
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
    ) -> RouterServerResult<u64> {
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
                        details_json,
                        usd_valuation_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
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
                        details_json,
                        usd_valuation_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
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
                    .bind(leg.details.clone())
                    .bind(leg.usd_valuation.clone())
                    .bind(leg.created_at)
                    .bind(leg.updated_at)
                    .execute(&mut *tx)
                    .await?;
                inserted += result.rows_affected();
            }

            tx.commit().await?;
            Ok::<u64, RouterServerError>(inserted)
        }
        .await;
        telemetry::record_db_query(
            "order.create_execution_legs_idempotent",
            result.is_ok(),
            started.elapsed(),
        );
        result
    }

    pub async fn create_execution_step(&self, step: &OrderExecutionStep) -> RouterServerResult<()> {
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
    ) -> RouterServerResult<u64> {
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
                inserted += result.rows_affected();
            }

            tx.commit().await?;
            Ok::<u64, RouterServerError>(inserted)
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
    ) -> RouterServerResult<Vec<OrderExecutionStep>> {
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

    pub async fn get_execution_legs_for_attempt(
        &self,
        execution_attempt_id: Uuid,
    ) -> RouterServerResult<Vec<OrderExecutionLeg>> {
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
    ) -> RouterServerResult<Vec<OrderExecutionStep>> {
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
    ) -> RouterServerResult<Vec<OrderExecutionLeg>> {
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

    pub async fn get_execution_step(&self, id: Uuid) -> RouterServerResult<OrderExecutionStep> {
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
    ) -> RouterServerResult<Option<OrderExecutionLeg>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH action_summary AS (
                SELECT
                    COUNT(*) AS total_actions,
                    BOOL_OR(status = 'failed') AS has_failed,
                    BOOL_OR(status = 'cancelled') AS has_cancelled,
                    BOOL_OR(status = 'running') AS has_running,
                    BOOL_OR(status = 'waiting') AS has_waiting,
                    BOOL_OR(status = 'ready') AS has_ready,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed_actions,
                    COUNT(*) FILTER (WHERE status IN ('completed', 'skipped')) AS terminal_actions,
                    MIN(started_at) FILTER (WHERE started_at IS NOT NULL) AS first_started_at,
                    MAX(completed_at) FILTER (WHERE completed_at IS NOT NULL) AS last_completed_at
                FROM order_execution_steps
                WHERE execution_leg_id = $1
            ),
            last_success_action AS (
                SELECT
                    response_json
                FROM order_execution_steps
                WHERE execution_leg_id = $1
                  AND status IN ('completed', 'skipped')
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
                    last_success_action.response_json
                FROM action_summary
                LEFT JOIN last_success_action ON true
            )
            UPDATE order_execution_legs leg
            SET
                status = rolled_up.status,
                started_at = COALESCE(leg.started_at, rolled_up.first_started_at),
                completed_at = CASE
                    WHEN rolled_up.status IN ('completed', 'failed', 'cancelled')
                        THEN COALESCE(leg.completed_at, rolled_up.last_completed_at, now())
                    ELSE NULL
                END,
                actual_amount_in = CASE
                    WHEN rolled_up.completed THEN leg.amount_in
                    ELSE NULL
                END,
                actual_amount_out = CASE
                    WHEN rolled_up.completed THEN COALESCE(
                        rolled_up.response_json->>'amount_out',
                        rolled_up.response_json->>'amountOut',
                        rolled_up.response_json #>> '{{response,amount_out}}',
                        rolled_up.response_json #>> '{{response,amountOut}}',
                        rolled_up.response_json #>> '{{response,expectedOutputAmount}}',
                        rolled_up.response_json->>'expectedOutputAmount',
                        rolled_up.response_json->>'output_amount',
                        rolled_up.response_json->>'outputAmount',
                        leg.expected_amount_out
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

    async fn refresh_execution_leg_for_step(
        &self,
        step: &OrderExecutionStep,
    ) -> RouterServerResult<()> {
        if let Some(execution_leg_id) = step.execution_leg_id {
            let _ = self
                .refresh_execution_leg_from_actions(execution_leg_id)
                .await?;
        }
        Ok(())
    }

    pub async fn skip_execution_steps_after_index(
        &self,
        execution_attempt_id: Uuid,
        after_step_index: i32,
        reason: serde_json::Value,
        updated_at: DateTime<Utc>,
    ) -> RouterServerResult<Vec<OrderExecutionStep>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'skipped',
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
            "order.skip_execution_steps_after_index",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;

        let steps = rows
            .iter()
            .map(|row| self.map_execution_step_row(row))
            .collect::<RouterServerResult<Vec<_>>>()?;
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
    ) -> RouterServerResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = $2,
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
    ) -> RouterServerResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'completed',
                response_json = $2,
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

    pub async fn complete_wait_for_deposit_step_for_order(
        &self,
        order_id: Uuid,
        response: serde_json::Value,
        tx_hash: Option<String>,
        completed_at: DateTime<Utc>,
    ) -> RouterServerResult<Option<OrderExecutionStep>> {
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
    ) -> RouterServerResult<OrderExecutionStep> {
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
    ) -> RouterServerResult<OrderExecutionStep> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            UPDATE order_execution_steps
            SET
                status = 'completed',
                response_json = $2,
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
    ) -> RouterServerResult<OrderExecutionStep> {
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

    pub async fn fail_observed_execution_step(
        &self,
        id: Uuid,
        error: serde_json::Value,
        failed_at: DateTime<Utc>,
    ) -> RouterServerResult<OrderExecutionStep> {
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

    fn map_order_row(&self, row: &sqlx_postgres::PgRow) -> RouterServerResult<RouterOrder> {
        let order_type = row.get::<String, _>("order_type");
        let order_type = RouterOrderType::from_db_string(&order_type).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported router order type: {order_type}"),
            }
        })?;

        let status = row.get::<String, _>("status");
        let status = RouterOrderStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
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
    ) -> RouterServerResult<RouterOrderAction> {
        match order_type {
            RouterOrderType::MarketOrder => {
                let order_kind = row
                    .get::<Option<String>, _>("market_order_kind")
                    .ok_or_else(|| RouterServerError::InvalidData {
                        message: "market order row is missing market_order_actions".to_string(),
                    })?;
                let order_kind =
                    MarketOrderKindType::from_db_string(&order_kind).ok_or_else(|| {
                        RouterServerError::InvalidData {
                            message: format!("unsupported market order kind: {order_kind}"),
                        }
                    })?;
                let order_kind = match order_kind {
                    MarketOrderKindType::ExactIn => MarketOrderKind::ExactIn {
                        amount_in: required_action_amount(row, "market_order_amount_in")?,
                        min_amount_out: required_action_amount(row, "market_order_min_amount_out")?,
                    },
                    MarketOrderKindType::ExactOut => MarketOrderKind::ExactOut {
                        amount_out: required_action_amount(row, "market_order_amount_out")?,
                        max_amount_in: required_action_amount(row, "market_order_max_amount_in")?,
                    },
                };
                let slippage_bps = u64::try_from(row.get::<i64, _>("market_order_slippage_bps"))
                    .map_err(|err| RouterServerError::InvalidData {
                        message: format!("invalid market order slippage_bps: {err}"),
                    })?;

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
                    .ok_or_else(|| RouterServerError::InvalidData {
                        message: "limit order row is missing limit_order_actions".to_string(),
                    })?;
                let residual_policy = LimitOrderResidualPolicy::from_db_string(&residual_policy)
                    .ok_or_else(|| RouterServerError::InvalidData {
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

    fn map_quote_row(&self, row: &sqlx_postgres::PgRow) -> RouterServerResult<MarketOrderQuote> {
        let order_kind = row.get::<String, _>("order_kind");
        let order_kind = MarketOrderKindType::from_db_string(&order_kind).ok_or_else(|| {
            RouterServerError::InvalidData {
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
            slippage_bps: u64::try_from(row.get::<i64, _>("slippage_bps")).map_err(|err| {
                RouterServerError::InvalidData {
                    message: format!("invalid quote slippage_bps: {err}"),
                }
            })?,
            provider_quote: row.get("provider_quote"),
            usd_valuation: row.get("usd_valuation_json"),
            expires_at: row.get("expires_at"),
            created_at: row.get("created_at"),
        })
    }

    fn map_limit_quote_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterServerResult<LimitOrderQuote> {
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
            .ok_or_else(|| RouterServerError::InvalidData {
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
            expires_at: row.get("expires_at"),
            created_at: row.get("created_at"),
        })
    }

    fn map_custody_vault_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterServerResult<CustodyVault> {
        let role = row.get::<String, _>("role");
        let role = CustodyVaultRole::from_db_string(&role).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported custody vault role: {role}"),
            }
        })?;
        let visibility = row.get::<String, _>("visibility");
        let visibility = CustodyVaultVisibility::from_db_string(&visibility).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported custody vault visibility: {visibility}"),
            }
        })?;
        let control_type = row.get::<String, _>("control_type");
        let control_type =
            CustodyVaultControlType::from_db_string(&control_type).ok_or_else(|| {
                RouterServerError::InvalidData {
                    message: format!("unsupported custody vault control type: {control_type}"),
                }
            })?;
        let status = row.get::<String, _>("status");
        let status = CustodyVaultStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
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
                salt.try_into().map_err(|_| RouterServerError::InvalidData {
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
    ) -> RouterServerResult<OrderProviderOperation> {
        let operation_type = row.get::<String, _>("operation_type");
        let operation_type =
            ProviderOperationType::from_db_string(&operation_type).ok_or_else(|| {
                RouterServerError::InvalidData {
                    message: format!("unsupported provider operation type: {operation_type}"),
                }
            })?;
        let status = row.get::<String, _>("status");
        let status = ProviderOperationStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
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
    ) -> RouterServerResult<OrderProviderOperationHint> {
        let hint_kind = row.get::<String, _>("hint_kind");
        let hint_kind = ProviderOperationHintKind::from_db_string(&hint_kind).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported provider operation hint kind: {hint_kind}"),
            }
        })?;
        let status = row.get::<String, _>("status");
        let status = ProviderOperationHintStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
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
    ) -> RouterServerResult<OrderProviderAddress> {
        let role = row.get::<String, _>("role");
        let role = ProviderAddressRole::from_db_string(&role).ok_or_else(|| {
            RouterServerError::InvalidData {
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
    ) -> RouterServerResult<OrderExecutionStep> {
        let step_type = row.get::<String, _>("step_type");
        let step_type = OrderExecutionStepType::from_db_string(&step_type).ok_or_else(|| {
            RouterServerError::InvalidData {
                message: format!("unsupported order execution step type: {step_type}"),
            }
        })?;
        let status = row.get::<String, _>("status");
        let status = OrderExecutionStepStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
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
    ) -> RouterServerResult<OrderExecutionLeg> {
        let status = row.get::<String, _>("status");
        let status = OrderExecutionStepStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
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
            details: row.get("details_json"),
            usd_valuation: row.get("usd_valuation_json"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    fn map_execution_attempt_row(
        &self,
        row: &sqlx_postgres::PgRow,
    ) -> RouterServerResult<OrderExecutionAttempt> {
        let attempt_kind = row.get::<String, _>("attempt_kind");
        let attempt_kind =
            OrderExecutionAttemptKind::from_db_string(&attempt_kind).ok_or_else(|| {
                RouterServerError::InvalidData {
                    message: format!("unsupported order execution attempt kind: {attempt_kind}"),
                }
            })?;
        let status = row.get::<String, _>("status");
        let status = OrderExecutionAttemptStatus::from_db_string(&status).ok_or_else(|| {
            RouterServerError::InvalidData {
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

fn parse_chain_id(value: String, label: &str) -> RouterServerResult<ChainId> {
    ChainId::parse(value).map_err(|err| RouterServerError::InvalidData {
        message: format!("unsupported {label} chain id: {err}"),
    })
}

fn parse_asset_id(value: String, label: &str) -> RouterServerResult<AssetId> {
    AssetId::parse(value).map_err(|err| RouterServerError::InvalidData {
        message: format!("unsupported {label} asset id: {err}"),
    })
}

fn parse_optional_deposit_asset(
    chain: Option<String>,
    asset: Option<String>,
    label: &str,
) -> RouterServerResult<Option<DepositAsset>> {
    match (chain, asset) {
        (Some(chain), Some(asset)) => Ok(Some(DepositAsset {
            chain: parse_chain_id(chain, label)?,
            asset: parse_asset_id(asset, label)?,
        })),
        (None, None) => Ok(None),
        _ => Err(RouterServerError::InvalidData {
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
    slippage_bps: u64,
}

fn market_order_action_fields(
    action: &RouterOrderAction,
) -> RouterServerResult<MarketOrderActionFields> {
    match action {
        RouterOrderAction::MarketOrder(action) => match &action.order_kind {
            MarketOrderKind::ExactIn {
                amount_in,
                min_amount_out,
            } => Ok(MarketOrderActionFields {
                order_kind: MarketOrderKindType::ExactIn,
                amount_in: Some(amount_in.clone()),
                min_amount_out: Some(min_amount_out.clone()),
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
                max_amount_in: Some(max_amount_in.clone()),
                slippage_bps: action.slippage_bps,
            }),
        },
        RouterOrderAction::LimitOrder(_) => Err(RouterServerError::InvalidData {
            message: "expected market order action".to_string(),
        }),
    }
}

fn limit_order_action_fields(action: &RouterOrderAction) -> RouterServerResult<LimitOrderAction> {
    match action {
        RouterOrderAction::LimitOrder(action) => Ok(action.clone()),
        RouterOrderAction::MarketOrder(_) => Err(RouterServerError::InvalidData {
            message: "expected limit order action".to_string(),
        }),
    }
}

fn required_action_amount(
    row: &sqlx_postgres::PgRow,
    column: &'static str,
) -> RouterServerResult<String> {
    row.get::<Option<String>, _>(column)
        .ok_or_else(|| RouterServerError::InvalidData {
            message: format!("order action is missing required {column}"),
        })
}
