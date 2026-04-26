use crate::{
    db::Database,
    error::RouterServerError,
    models::{
        CustodyVault, CustodyVaultControlType, CustodyVaultRole, CustodyVaultStatus,
        CustodyVaultVisibility, DepositVault, DepositVaultStatus, MarketOrderKind,
        OrderExecutionAttempt, OrderExecutionAttemptKind, OrderExecutionAttemptStatus,
        OrderExecutionStep, OrderExecutionStepStatus, OrderExecutionStepType, OrderProviderAddress,
        OrderProviderOperation, OrderProviderOperationHint, ProviderAddressRole,
        ProviderOperationHintKind, ProviderOperationHintStatus, ProviderOperationStatus,
        ProviderOperationType, RouterOrder, RouterOrderStatus,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{
            ActionProviderRegistry, BridgeProvider, BridgeQuoteRequest, ExchangeProvider,
            ExchangeQuoteRequest, ProviderExecutionIntent, ProviderExecutionState,
            ProviderExecutionStatePatch, ProviderOperationObservation,
            ProviderOperationObservationRequest, UnitProvider,
        },
        asset_registry::{
            CanonicalAsset, MarketOrderNode, MarketOrderTransitionKind, ProviderId, TransitionDecl,
            TransitionPath,
        },
        custody_action_executor::{
            ChainCall, CustodyAction, CustodyActionError, CustodyActionExecutor,
            CustodyActionReceipt, CustodyActionRequest, HyperliquidCall, HyperliquidCallNetwork,
            HyperliquidCallPayload, ReleasedSweepResult,
        },
        market_order_planner::{MarketOrderRoutePlanError, MarketOrderRoutePlanner},
        provider_policy::ProviderPolicyService,
    },
    telemetry,
};
use alloy::primitives::U256;
use async_trait::async_trait;
use chains::{ChainRegistry, UserDepositCandidateStatus};
use chrono::{Duration as ChronoDuration, Utc};
use hyperliquid_client::actions::{
    Actions as HyperliquidActions, BulkCancel, CancelRequest as HyperliquidCancelRequest,
};
use router_primitives::{ChainType, Currency, TokenIdentifier};
use serde_json::{json, Value};
use snafu::Snafu;
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum OrderExecutionError {
    #[snafu(display("Database error: {}", source))]
    Database { source: RouterServerError },

    #[snafu(display("Market-order route planning error: {}", source))]
    RoutePlan { source: MarketOrderRoutePlanError },

    #[snafu(display("order {} does not have a funding vault", order_id))]
    MissingFundingVault { order_id: Uuid },

    #[snafu(display("vault {} is not funded yet", vault_id))]
    FundingVaultNotFunded { vault_id: Uuid },

    #[snafu(display("order {} is not ready for execution planning", order_id))]
    OrderNotReady { order_id: Uuid },

    #[snafu(display("execution step {} is not executable", step_id))]
    StepNotExecutable { step_id: Uuid },

    #[snafu(display(
        "provider status update must include provider_operation_id or provider plus provider_ref"
    ))]
    MissingProviderStatusUpdateSelector,

    #[snafu(display(
        "provider status update for operation {} does not match provider {}",
        operation_id,
        provider
    ))]
    ProviderStatusUpdateProviderMismatch {
        operation_id: Uuid,
        provider: String,
    },

    #[snafu(display("provider {} request failed: {}", provider, message))]
    ProviderRequestFailed { provider: String, message: String },

    #[snafu(display("custody action execution failed: {}", source))]
    CustodyAction { source: CustodyActionError },

    #[snafu(display(
        "provider {} returned a custody action, but no custody executor is configured",
        provider
    ))]
    CustodyExecutorUnavailable { provider: String },

    #[snafu(display(
        "internal execution step {} is not executable by the order executor",
        step_type
    ))]
    InternalStepNotExecutable { step_type: &'static str },

    #[snafu(display("provider operation hint {} was invalid: {}", hint_id, reason))]
    InvalidProviderOperationHint { hint_id: Uuid, reason: String },

    #[snafu(display(
        "execution step {} violated the intermediate custody invariant: {}",
        step_id,
        reason
    ))]
    IntermediateCustodyInvariant { step_id: Uuid, reason: String },

    #[snafu(display(
        "provider policy blocked {} for provider {}: {} ({})",
        phase,
        provider,
        state,
        reason
    ))]
    ProviderPolicyBlocked {
        provider: String,
        phase: &'static str,
        state: String,
        reason: String,
    },
}

pub type OrderExecutionResult<T> = Result<T, OrderExecutionError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderPlanMaterialization {
    pub order_id: Uuid,
    pub plan_kind: String,
    pub planned_steps: usize,
    pub inserted_steps: u64,
}

#[derive(Debug, Clone)]
pub struct OrderExecutionSummary {
    pub order_id: Uuid,
    pub completed_steps: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderWorkerPassSummary {
    pub reconciled_failed_orders: usize,
    pub processed_provider_hints: usize,
    pub maintenance_tasks: usize,
    pub planned_orders: usize,
    pub executed_orders: usize,
}

impl OrderWorkerPassSummary {
    fn add(&mut self, other: OrderWorkerPassSummary) {
        self.reconciled_failed_orders += other.reconciled_failed_orders;
        self.processed_provider_hints += other.processed_provider_hints;
        self.maintenance_tasks += other.maintenance_tasks;
        self.planned_orders += other.planned_orders;
        self.executed_orders += other.executed_orders;
    }

    #[must_use]
    pub fn has_activity(&self) -> bool {
        self.reconciled_failed_orders > 0
            || self.processed_provider_hints > 0
            || self.maintenance_tasks > 0
            || self.planned_orders > 0
            || self.executed_orders > 0
    }
}

#[derive(Debug, Clone)]
pub struct ProviderOperationStatusUpdate {
    pub provider_operation_id: Option<Uuid>,
    pub provider: Option<String>,
    pub provider_ref: Option<String>,
    pub status: ProviderOperationStatus,
    pub observed_state: Value,
    pub response: Option<Value>,
    pub tx_hash: Option<String>,
    pub error: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ProviderOperationStatusUpdateOutcome {
    pub operation: OrderProviderOperation,
    pub step: Option<OrderExecutionStep>,
    pub order: RouterOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderExecutionCrashPoint {
    AfterStepMarkedFailed,
    AfterExecutionStepStatusPersisted,
    AfterProviderOperationStatusPersisted,
    AfterProviderStepSettlement,
}

pub trait OrderExecutionCrashInjector: Send + Sync {
    fn trigger(&self, point: OrderExecutionCrashPoint);
}

#[derive(Default)]
struct NoopOrderExecutionCrashInjector;

impl OrderExecutionCrashInjector for NoopOrderExecutionCrashInjector {
    fn trigger(&self, _point: OrderExecutionCrashPoint) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HintDisposition {
    Processed,
    Ignored { reason: String },
}

#[derive(Debug, Clone)]
enum RefundSourceHandle {
    FundingVault(DepositVault),
    ExternalCustody(CustodyVault),
    HyperliquidSpot {
        vault: CustodyVault,
        coin: String,
        canonical: CanonicalAsset,
    },
}

#[derive(Debug, Clone)]
struct RefundSourcePosition {
    handle: RefundSourceHandle,
    asset: DepositAsset,
    amount: String,
}

enum ProviderOperationHintVerification {
    StatusUpdate(ProviderOperationStatusUpdate),
}

#[async_trait]
trait ProviderOperationVerifier: Send + Sync {
    async fn verify(
        &self,
        manager: &OrderExecutionManager,
        hint: &OrderProviderOperationHint,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<ProviderOperationHintVerification>;
}

struct UnitDepositVerifier;
struct ProviderStatusVerifier;

static UNIT_DEPOSIT_VERIFIER: UnitDepositVerifier = UnitDepositVerifier;
static PROVIDER_STATUS_VERIFIER: ProviderStatusVerifier = ProviderStatusVerifier;

const MAX_EXECUTION_ATTEMPTS: i32 = 2;
const REFUND_PATH_MAX_DEPTH: usize = 5;
const REFUND_TOP_K_PATHS: usize = 8;
const REFUND_PROVIDER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 = 1_000_000;

#[async_trait]
impl ProviderOperationVerifier for UnitDepositVerifier {
    async fn verify(
        &self,
        manager: &OrderExecutionManager,
        hint: &OrderProviderOperationHint,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<ProviderOperationHintVerification> {
        manager.verify_unit_deposit_hint(hint, operation).await
    }
}

#[async_trait]
impl ProviderOperationVerifier for ProviderStatusVerifier {
    async fn verify(
        &self,
        manager: &OrderExecutionManager,
        hint: &OrderProviderOperationHint,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<ProviderOperationHintVerification> {
        manager.verify_provider_status_hint(hint, operation).await
    }
}

#[derive(Clone)]
pub struct OrderExecutionManager {
    db: Database,
    market_order_planner: MarketOrderRoutePlanner,
    action_providers: Arc<ActionProviderRegistry>,
    custody_action_executor: Option<Arc<CustodyActionExecutor>>,
    chain_registry: Option<Arc<ChainRegistry>>,
    provider_policies: Option<Arc<ProviderPolicyService>>,
    crash_injector: Arc<dyn OrderExecutionCrashInjector>,
}

impl OrderExecutionManager {
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self::with_action_providers(db, Arc::new(ActionProviderRegistry::default()))
    }

    #[must_use]
    pub fn with_action_providers(
        db: Database,
        action_providers: Arc<ActionProviderRegistry>,
    ) -> Self {
        let market_order_planner = MarketOrderRoutePlanner::new(action_providers.asset_registry());
        Self {
            db,
            market_order_planner,
            action_providers,
            custody_action_executor: None,
            chain_registry: None,
            provider_policies: None,
            crash_injector: Arc::new(NoopOrderExecutionCrashInjector),
        }
    }

    #[must_use]
    pub fn with_action_providers_and_chain_registry(
        db: Database,
        action_providers: Arc<ActionProviderRegistry>,
        chain_registry: Arc<ChainRegistry>,
    ) -> Self {
        let market_order_planner = MarketOrderRoutePlanner::new(action_providers.asset_registry());
        Self {
            db,
            market_order_planner,
            action_providers,
            custody_action_executor: None,
            chain_registry: Some(chain_registry),
            provider_policies: None,
            crash_injector: Arc::new(NoopOrderExecutionCrashInjector),
        }
    }

    #[must_use]
    pub fn with_dependencies(
        db: Database,
        action_providers: Arc<ActionProviderRegistry>,
        custody_action_executor: Arc<CustodyActionExecutor>,
        chain_registry: Arc<ChainRegistry>,
    ) -> Self {
        let market_order_planner = MarketOrderRoutePlanner::new(action_providers.asset_registry());
        Self {
            db,
            market_order_planner,
            action_providers,
            custody_action_executor: Some(custody_action_executor),
            chain_registry: Some(chain_registry),
            provider_policies: None,
            crash_injector: Arc::new(NoopOrderExecutionCrashInjector),
        }
    }

    #[must_use]
    pub fn with_provider_policies(
        mut self,
        provider_policies: Option<Arc<ProviderPolicyService>>,
    ) -> Self {
        self.provider_policies = provider_policies;
        self
    }

    #[must_use]
    pub fn with_crash_injector(
        mut self,
        crash_injector: Arc<dyn OrderExecutionCrashInjector>,
    ) -> Self {
        self.crash_injector = crash_injector;
        self
    }

    pub async fn process_worker_pass(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<OrderWorkerPassSummary> {
        let reconciled_failed_orders = self.reconcile_failed_orders(limit).await?;
        let processed_provider_hints = self.process_provider_operation_hints(limit).await?;
        let terminal_provider_recoveries = self
            .process_terminal_provider_operation_recovery(limit)
            .await?;
        let completed_order_finalizations = self
            .process_completed_orders_pending_finalization(limit)
            .await?;
        let direct_refund_finalizations =
            self.process_direct_refund_finalization_pass(limit).await?;
        let refund_plans = self.process_refund_planning_pass(limit).await?;
        let manual_refund_alignments = self
            .process_manual_refund_vault_alignment_pass(limit)
            .await?;
        let materialized = self.process_planning_pass(limit).await?;
        let executable_orders = self
            .db
            .orders()
            .get_market_orders_ready_for_execution(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut executed_orders = 0_usize;
        for order in executable_orders {
            match self.execute_materialized_order(order.id).await {
                Ok(Some(_)) => executed_orders += 1,
                Ok(None) => {
                    // No-op: another worker drove this order (or it was already
                    // terminal). Not counted as "executed" to avoid over-reporting
                    // under concurrent worker passes.
                }
                Err(err) => {
                    warn!(
                        order_id = %order.id,
                        error = %err,
                        "order execution failed in worker pass",
                    );
                }
            }
        }
        let terminal_custody_finalizations =
            self.process_terminal_internal_custody_vaults(limit).await?;
        let released_custody_sweeps = self.process_released_internal_custody_sweeps(limit).await?;

        Ok(OrderWorkerPassSummary {
            reconciled_failed_orders,
            processed_provider_hints,
            maintenance_tasks: terminal_provider_recoveries
                + completed_order_finalizations
                + direct_refund_finalizations
                + refund_plans
                + manual_refund_alignments
                + terminal_custody_finalizations
                + released_custody_sweeps,
            planned_orders: materialized.len(),
            executed_orders,
        })
    }

    pub async fn process_order_ids(
        &self,
        order_ids: &[Uuid],
    ) -> OrderExecutionResult<OrderWorkerPassSummary> {
        let mut summary = OrderWorkerPassSummary::default();
        for order_id in order_ids {
            match self.process_order_work(*order_id).await {
                Ok(order_summary) => summary.add(order_summary),
                Err(err) => {
                    warn!(
                        order_id = %order_id,
                        error = %err,
                        "order-specific execution wakeup failed",
                    );
                }
            }
        }
        Ok(summary)
    }

    async fn process_order_work(
        &self,
        order_id: Uuid,
    ) -> OrderExecutionResult<OrderWorkerPassSummary> {
        let mut summary = OrderWorkerPassSummary::default();
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;

        if self
            .finalize_direct_refund_if_complete(order.clone())
            .await?
        {
            summary.maintenance_tasks += 1;
        }

        let refreshed = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if self.align_manual_refund_vault_for_order(&refreshed).await? {
            summary.maintenance_tasks += 1;
        }

        if self.materialize_refund_plan_for_order(order_id).await? {
            summary.planned_orders += 1;
        }

        match self.materialize_plan_for_order(order_id).await {
            Ok(_) => summary.planned_orders += 1,
            Err(OrderExecutionError::OrderNotReady { .. })
            | Err(OrderExecutionError::FundingVaultNotFunded { .. }) => {}
            Err(err) => return Err(err),
        }

        match self.execute_materialized_order(order_id).await {
            Ok(Some(_)) => summary.executed_orders += 1,
            Ok(None) => {}
            Err(OrderExecutionError::OrderNotReady { .. }) => {}
            Err(err) => return Err(err),
        }

        let refreshed = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        summary.maintenance_tasks += self
            .finalize_internal_custody_vaults_for_order(&refreshed)
            .await?;

        Ok(summary)
    }

    async fn process_terminal_provider_operation_recovery(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<usize> {
        let operations = self
            .db
            .orders()
            .find_terminal_provider_operations_pending_step_settlement(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut recovered = 0usize;
        for operation in operations {
            match self.recover_terminal_provider_operation(&operation).await {
                Ok(true) => recovered += 1,
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        provider_operation_id = %operation.id,
                        provider = %operation.provider,
                        error = %err,
                        "terminal provider-operation recovery failed in worker pass",
                    );
                }
            }
        }
        Ok(recovered)
    }

    async fn recover_terminal_provider_operation(
        &self,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<bool> {
        let step = self
            .apply_provider_status_update_to_step(
                operation,
                operation.observed_state.clone(),
                Some(operation.response.clone()),
                persisted_provider_operation_tx_hash(operation),
                None,
            )
            .await?;
        let _ = self
            .settle_order_after_provider_status_update(operation)
            .await?;
        Ok(step.is_some())
    }

    async fn process_completed_orders_pending_finalization(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<usize> {
        let orders = self
            .db
            .orders()
            .find_executing_orders_pending_completion_finalization(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut finalized = 0usize;
        for order in orders {
            match self.finalize_order_if_complete(order).await {
                Ok(updated) if updated.status == RouterOrderStatus::Completed => finalized += 1,
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        error = %err,
                        "completed-order finalization recovery failed in worker pass",
                    );
                }
            }
        }
        Ok(finalized)
    }

    async fn process_direct_refund_finalization_pass(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<usize> {
        let orders = self
            .db
            .orders()
            .find_refunding_orders_pending_direct_refund_finalization(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut finalized = 0usize;
        for order in orders {
            match self.finalize_direct_refund_if_complete(order).await {
                Ok(true) => finalized += 1,
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        error = %err,
                        "direct-refund finalization recovery failed in worker pass",
                    );
                }
            }
        }
        Ok(finalized)
    }

    async fn process_refund_planning_pass(&self, limit: i64) -> OrderExecutionResult<usize> {
        let orders = self
            .db
            .orders()
            .find_orders_pending_refund_planning(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut planned = 0usize;
        for order in orders {
            match self.materialize_refund_plan_for_order(order.id).await {
                Ok(true) => planned += 1,
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        order_id = %order.id,
                        error = %err,
                        "refund planning failed in worker pass",
                    );
                }
            }
        }
        Ok(planned)
    }

    async fn process_manual_refund_vault_alignment_pass(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<usize> {
        let orders = self
            .db
            .orders()
            .find_orders_pending_manual_refund_vault_alignment(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut aligned = 0usize;
        for order in orders {
            if self.align_manual_refund_vault_for_order(&order).await? {
                aligned += 1;
            }
        }
        Ok(aligned)
    }

    async fn align_manual_refund_vault_for_order(
        &self,
        order: &RouterOrder,
    ) -> OrderExecutionResult<bool> {
        if order.status != RouterOrderStatus::RefundManualInterventionRequired {
            return Ok(false);
        }
        let Some(funding_vault_id) = order.funding_vault_id else {
            return Ok(false);
        };
        let funding_vault = self
            .db
            .vaults()
            .get(funding_vault_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if !matches!(
            funding_vault.status,
            DepositVaultStatus::RefundRequired | DepositVaultStatus::Refunding
        ) {
            return Ok(false);
        }
        self.db
            .vaults()
            .transition_status(
                funding_vault_id,
                funding_vault.status,
                DepositVaultStatus::RefundManualInterventionRequired,
                Utc::now(),
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        Ok(true)
    }

    /// Retry/refund-decision backstop.
    ///
    /// Scans for orders whose latest execution attempt failed and have not yet
    /// been moved into either a retry attempt or refund-required state. This
    /// converges retry/refund decisions after crashes between step failure and
    /// attempt handoff.
    pub async fn reconcile_failed_orders(&self, limit: i64) -> OrderExecutionResult<usize> {
        let candidates = self
            .db
            .orders()
            .find_orders_pending_retry_or_refund_decision(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut reconciled = 0_usize;
        for order in candidates {
            match self.process_failed_attempt_for_order(order.id).await {
                Ok(true) => reconciled += 1,
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        order_id = %order.id,
                        error = %err,
                        "failed to reconcile retry/refund decision for failed attempt",
                    );
                }
            }
        }
        Ok(reconciled)
    }

    async fn process_failed_attempt_for_order(&self, order_id: Uuid) -> OrderExecutionResult<bool> {
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if matches!(
            order.status,
            RouterOrderStatus::Completed
                | RouterOrderStatus::RefundRequired
                | RouterOrderStatus::Refunded
                | RouterOrderStatus::RefundManualInterventionRequired
                | RouterOrderStatus::Failed
                | RouterOrderStatus::Expired
        ) {
            return Ok(false);
        }

        let mut latest_attempt = self.ensure_failed_latest_attempt(&order).await?;
        let steps = self
            .db
            .orders()
            .get_execution_steps_for_attempt(latest_attempt.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let failed_step = steps
            .iter()
            .filter(|step| step.status == OrderExecutionStepStatus::Failed)
            .max_by_key(|step| step.step_index)
            .cloned()
            .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                provider: "internal".to_string(),
                message: format!(
                    "order {order_id} latest failed attempt {} has no failed step",
                    latest_attempt.id
                ),
            })?;
        let trigger_provider_operation_id = self
            .db
            .orders()
            .get_provider_operations(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
            .into_iter()
            .filter(|operation| operation.execution_step_id == Some(failed_step.id))
            .map(|operation| (operation.created_at, operation.id))
            .max_by_key(|(created_at, _)| *created_at)
            .map(|(_, id)| id);
        if latest_attempt.trigger_provider_operation_id.is_none()
            && trigger_provider_operation_id.is_some()
        {
            latest_attempt.trigger_provider_operation_id = trigger_provider_operation_id;
        }

        if latest_attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery {
            return self
                .mark_order_refund_manual_intervention_required(
                    &order,
                    &latest_attempt,
                    &failed_step,
                )
                .await;
        }

        if latest_attempt.attempt_index < MAX_EXECUTION_ATTEMPTS {
            return self
                .spawn_retry_attempt(&order, &latest_attempt, &steps, &failed_step)
                .await;
        }

        self.mark_order_refund_required(&order, &latest_attempt, &failed_step)
            .await
    }

    async fn ensure_failed_latest_attempt(
        &self,
        order: &RouterOrder,
    ) -> OrderExecutionResult<OrderExecutionAttempt> {
        if let Some(active_attempt) = self
            .db
            .orders()
            .get_active_execution_attempt(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
        {
            let steps = self
                .db
                .orders()
                .get_execution_steps_for_attempt(active_attempt.id)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            if let Some(failed_step) = steps
                .iter()
                .filter(|step| step.status == OrderExecutionStepStatus::Failed)
                .max_by_key(|step| step.step_index)
            {
                let snapshot = failed_attempt_snapshot(order, failed_step);
                let trigger_provider_operation_id = self
                    .db
                    .orders()
                    .get_provider_operations(order.id)
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?
                    .into_iter()
                    .filter(|operation| operation.execution_step_id == Some(failed_step.id))
                    .map(|operation| (operation.created_at, operation.id))
                    .max_by_key(|(created_at, _)| *created_at)
                    .map(|(_, id)| id);
                let failed_attempt = self
                    .db
                    .orders()
                    .mark_execution_attempt_failed(
                        active_attempt.id,
                        Some(failed_step.id),
                        trigger_provider_operation_id,
                        json!({
                            "reason": "execution_step_failed",
                            "step_id": failed_step.id,
                            "step_type": failed_step.step_type.to_db_string(),
                            "step_error": &failed_step.error,
                        }),
                        snapshot,
                        Utc::now(),
                    )
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?;
                return Ok(failed_attempt);
            }
        }

        self.db
            .orders()
            .get_latest_execution_attempt(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
            .filter(|attempt| attempt.status == OrderExecutionAttemptStatus::Failed)
            .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                provider: "internal".to_string(),
                message: format!("order {} is not pending a retry/refund decision", order.id),
            })
    }

    async fn spawn_retry_attempt(
        &self,
        order: &RouterOrder,
        failed_attempt: &OrderExecutionAttempt,
        steps: &[OrderExecutionStep],
        failed_step: &OrderExecutionStep,
    ) -> OrderExecutionResult<bool> {
        let now = Utc::now();
        let retry_attempt = OrderExecutionAttempt {
            id: Uuid::now_v7(),
            order_id: order.id,
            attempt_index: failed_attempt.attempt_index + 1,
            attempt_kind: OrderExecutionAttemptKind::RetryExecution,
            status: OrderExecutionAttemptStatus::Planning,
            trigger_step_id: Some(failed_step.id),
            trigger_provider_operation_id: failed_attempt.trigger_provider_operation_id,
            failure_reason: json!({
                "reason": "retry_after_failed_attempt",
                "failed_attempt_id": failed_attempt.id,
                "failed_attempt_index": failed_attempt.attempt_index,
                "failed_step_id": failed_step.id,
                "failed_step_type": failed_step.step_type.to_db_string(),
                "failed_step_error": &failed_step.error,
            }),
            input_custody_snapshot: failed_attempt_snapshot(order, failed_step),
            created_at: now,
            updated_at: now,
        };
        match self
            .db
            .orders()
            .create_execution_attempt(&retry_attempt)
            .await
        {
            Ok(()) => {}
            Err(RouterServerError::DatabaseQuery { source }) if is_unique_violation(&source) => {
                return Ok(false);
            }
            Err(source) => return Err(OrderExecutionError::Database { source }),
        }

        let _ = self
            .db
            .orders()
            .skip_execution_steps_after_index(
                failed_attempt.id,
                failed_step.step_index,
                json!({
                    "reason": "superseded_by_retry_attempt",
                    "retry_attempt_id": retry_attempt.id,
                    "retry_attempt_index": retry_attempt.attempt_index,
                }),
                now,
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;

        let mut retry_steps: Vec<OrderExecutionStep> = steps
            .iter()
            .filter(|step| {
                step.step_index >= failed_step.step_index
                    && matches!(
                        step.status,
                        OrderExecutionStepStatus::Failed
                            | OrderExecutionStepStatus::Planned
                            | OrderExecutionStepStatus::Ready
                            | OrderExecutionStepStatus::Waiting
                            | OrderExecutionStepStatus::Running
                    )
            })
            .cloned()
            .collect();
        for step in &mut retry_steps {
            step.id = Uuid::now_v7();
            step.execution_attempt_id = Some(retry_attempt.id);
            step.status = OrderExecutionStepStatus::Planned;
            step.tx_hash = None;
            step.provider_ref = None;
            step.attempt_count = 0;
            step.next_attempt_at = None;
            step.started_at = None;
            step.completed_at = None;
            step.response = json!({});
            step.error = json!({});
            step.created_at = now;
            step.updated_at = now;
            set_json_value(
                &mut step.details,
                "retry_attempt_from_attempt_id",
                json!(failed_attempt.id),
            );
            set_json_value(
                &mut step.details,
                "retry_attempt_from_attempt_index",
                json!(failed_attempt.attempt_index),
            );
            set_json_value(
                &mut step.details,
                "retry_attempt_trigger_step_id",
                json!(failed_step.id),
            );
            step.idempotency_key = execution_attempt_idempotency_key(
                order.id,
                retry_attempt.attempt_index,
                &step.provider,
                step.step_index,
            );
        }
        self.db
            .orders()
            .create_execution_steps_idempotent(&retry_steps)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let _ = self
            .db
            .orders()
            .transition_execution_attempt_status(
                retry_attempt.id,
                OrderExecutionAttemptStatus::Planning,
                OrderExecutionAttemptStatus::Active,
                now,
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        Ok(true)
    }

    async fn mark_order_refund_required(
        &self,
        order: &RouterOrder,
        failed_attempt: &OrderExecutionAttempt,
        failed_step: &OrderExecutionStep,
    ) -> OrderExecutionResult<bool> {
        let now = Utc::now();
        let refund_attempt = OrderExecutionAttempt {
            id: Uuid::now_v7(),
            order_id: order.id,
            attempt_index: failed_attempt.attempt_index + 1,
            attempt_kind: OrderExecutionAttemptKind::RefundRecovery,
            status: OrderExecutionAttemptStatus::RefundRequired,
            trigger_step_id: Some(failed_step.id),
            trigger_provider_operation_id: failed_attempt.trigger_provider_operation_id,
            failure_reason: json!({
                "reason": "refund_required_after_failed_attempts",
                "failed_attempt_id": failed_attempt.id,
                "failed_attempt_index": failed_attempt.attempt_index,
                "failed_step_id": failed_step.id,
                "failed_step_type": failed_step.step_type.to_db_string(),
                "failed_step_error": &failed_step.error,
            }),
            input_custody_snapshot: failed_attempt_snapshot(order, failed_step),
            created_at: now,
            updated_at: now,
        };
        match self
            .db
            .orders()
            .create_execution_attempt(&refund_attempt)
            .await
        {
            Ok(()) => {}
            Err(RouterServerError::DatabaseQuery { source }) if is_unique_violation(&source) => {
                return Ok(false);
            }
            Err(source) => return Err(OrderExecutionError::Database { source }),
        }

        let order_transitioned = match self
            .db
            .orders()
            .transition_status(
                order.id,
                RouterOrderStatus::Executing,
                RouterOrderStatus::RefundRequired,
                now,
            )
            .await
        {
            Ok(_) => true,
            Err(RouterServerError::NotFound) => match self
                .db
                .orders()
                .transition_status(
                    order.id,
                    RouterOrderStatus::Funded,
                    RouterOrderStatus::RefundRequired,
                    now,
                )
                .await
            {
                Ok(_) => true,
                Err(RouterServerError::NotFound) => false,
                Err(source) => return Err(OrderExecutionError::Database { source }),
            },
            Err(source) => return Err(OrderExecutionError::Database { source }),
        };
        if let Some(funding_vault_id) = order.funding_vault_id {
            match self
                .db
                .vaults()
                .transition_status(
                    funding_vault_id,
                    DepositVaultStatus::Executing,
                    DepositVaultStatus::RefundRequired,
                    now,
                )
                .await
            {
                Ok(_) | Err(RouterServerError::NotFound) => {}
                Err(source) => return Err(OrderExecutionError::Database { source }),
            }
            match self
                .db
                .vaults()
                .transition_status(
                    funding_vault_id,
                    DepositVaultStatus::Funded,
                    DepositVaultStatus::RefundRequired,
                    now,
                )
                .await
            {
                Ok(_) | Err(RouterServerError::NotFound) => {}
                Err(source) => return Err(OrderExecutionError::Database { source }),
            }
        }
        if order_transitioned {
            let order = self
                .db
                .orders()
                .get(order.id)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            let _ = self
                .finalize_internal_custody_vaults_for_order(&order)
                .await?;
        }
        Ok(true)
    }

    async fn mark_order_refund_manual_intervention_required(
        &self,
        order: &RouterOrder,
        failed_attempt: &OrderExecutionAttempt,
        failed_step: &OrderExecutionStep,
    ) -> OrderExecutionResult<bool> {
        self.mark_order_refund_manual_intervention(
            order,
            failed_attempt.id,
            json!({
                "reason": "refund_attempt_failed",
                "failed_attempt_id": failed_attempt.id,
                "failed_attempt_index": failed_attempt.attempt_index,
                "failed_step_id": failed_step.id,
                "failed_step_type": failed_step.step_type.to_db_string(),
                "failed_step_error": &failed_step.error,
            }),
            failed_attempt_snapshot(order, failed_step),
        )
        .await
    }

    async fn mark_order_refund_manual_intervention(
        &self,
        order: &RouterOrder,
        attempt_id: Uuid,
        failure_reason: Value,
        snapshot: Value,
    ) -> OrderExecutionResult<bool> {
        let now = Utc::now();
        match self
            .db
            .orders()
            .mark_execution_attempt_manual_intervention_required(
                attempt_id,
                failure_reason,
                snapshot,
                now,
            )
            .await
        {
            Ok(_) => {}
            Err(RouterServerError::NotFound) => return Ok(false),
            Err(source) => return Err(OrderExecutionError::Database { source }),
        }

        let refreshed = self
            .db
            .orders()
            .get(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let refreshed = match refreshed.status {
            RouterOrderStatus::RefundRequired | RouterOrderStatus::Refunding => self
                .db
                .orders()
                .transition_status(
                    refreshed.id,
                    refreshed.status,
                    RouterOrderStatus::RefundManualInterventionRequired,
                    now,
                )
                .await
                .map_err(|source| OrderExecutionError::Database { source })?,
            RouterOrderStatus::RefundManualInterventionRequired => refreshed,
            _ => refreshed,
        };

        if let Some(funding_vault_id) = refreshed.funding_vault_id {
            let funding_vault = self
                .db
                .vaults()
                .get(funding_vault_id)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            match funding_vault.status {
                DepositVaultStatus::RefundRequired | DepositVaultStatus::Refunding => {
                    let _ = self
                        .db
                        .vaults()
                        .transition_status(
                            funding_vault_id,
                            funding_vault.status,
                            DepositVaultStatus::RefundManualInterventionRequired,
                            now,
                        )
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                }
                DepositVaultStatus::RefundManualInterventionRequired => {}
                _ => {}
            }
        }
        let _ = self
            .finalize_internal_custody_vaults_for_order(&refreshed)
            .await?;
        Ok(true)
    }

    pub async fn record_provider_operation_hint(
        &self,
        hint: OrderProviderOperationHint,
    ) -> OrderExecutionResult<OrderProviderOperationHint> {
        self.db
            .orders()
            .get_provider_operation(hint.provider_operation_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        self.db
            .orders()
            .create_provider_operation_hint(&hint)
            .await
            .map_err(|source| OrderExecutionError::Database { source })
    }

    /// Load a provider operation by id and dispatch it to the matching provider's
    /// `observe_*` method. Used by the router-api observe proxy so detectors
    /// (Sauron) can ask the router to poll provider endpoints without having to
    /// hold direct URLs for every third-party API themselves.
    pub async fn observe_provider_operation(
        &self,
        operation_id: Uuid,
        hint_evidence: Value,
    ) -> OrderExecutionResult<Option<ProviderOperationObservation>> {
        let operation = self
            .db
            .orders()
            .get_provider_operation(operation_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let retry_hint_evidence = hint_evidence.clone();
        let request = ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence,
        };
        let provider_name = operation.provider.clone();
        match operation.operation_type {
            ProviderOperationType::AcrossBridge
            | ProviderOperationType::HyperliquidBridgeDeposit => {
                let provider = self
                    .action_providers
                    .bridge(&operation.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: provider_name.clone(),
                        message: "bridge provider is not configured".to_string(),
                    })?;
                provider
                    .observe_bridge_operation(request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: provider_name,
                        message,
                    })
            }
            ProviderOperationType::UnitDeposit | ProviderOperationType::UnitWithdrawal => {
                let provider =
                    self.action_providers
                        .unit(&operation.provider)
                        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                            provider: provider_name.clone(),
                            message: "unit provider is not configured".to_string(),
                        })?;
                provider
                    .observe_unit_operation(request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: provider_name,
                        message,
                    })
            }
            ProviderOperationType::HyperliquidTrade
            | ProviderOperationType::UniversalRouterSwap => {
                let provider = self
                    .action_providers
                    .exchange(&operation.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: provider_name.clone(),
                        message: "exchange provider is not configured".to_string(),
                    })?;
                let observation =
                    provider
                        .observe_trade_operation(request)
                        .await
                        .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                            provider: provider_name,
                            message,
                        })?;
                if hyperliquid_timeout_due(&operation, observation.as_ref()) {
                    self.cancel_timed_out_hyperliquid_trade(&operation).await?;
                    let retry_request = ProviderOperationObservationRequest {
                        operation_id: operation.id,
                        operation_type: operation.operation_type,
                        provider_ref: operation.provider_ref.clone(),
                        request: operation.request.clone(),
                        response: operation.response.clone(),
                        observed_state: operation.observed_state.clone(),
                        hint_evidence: retry_hint_evidence,
                    };
                    return provider
                        .observe_trade_operation(retry_request)
                        .await
                        .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                            provider: operation.provider.clone(),
                            message,
                        });
                }
                Ok(observation)
            }
        }
    }

    async fn cancel_timed_out_hyperliquid_trade(
        &self,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<()> {
        let custody_action_executor = self.custody_action_executor.clone().ok_or_else(|| {
            OrderExecutionError::CustodyExecutorUnavailable {
                provider: operation.provider.clone(),
            }
        })?;
        let custody_vault_id = operation
            .request
            .get("hyperliquid_custody_vault_id")
            .and_then(Value::as_str)
            .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                provider: operation.provider.clone(),
                message: "hyperliquid timeout cancel missing custody vault id".to_string(),
            })
            .and_then(|raw| {
                Uuid::parse_str(raw).map_err(|err| OrderExecutionError::ProviderRequestFailed {
                    provider: operation.provider.clone(),
                    message: format!("invalid hyperliquid timeout cancel custody vault id: {err}"),
                })
            })?;
        let target_base_url = operation
            .request
            .get("target_base_url")
            .and_then(Value::as_str)
            .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                provider: operation.provider.clone(),
                message: "hyperliquid timeout cancel missing target base url".to_string(),
            })?;
        let network: HyperliquidCallNetwork =
            serde_json::from_value(operation.request.get("network").cloned().ok_or_else(|| {
                OrderExecutionError::ProviderRequestFailed {
                    provider: operation.provider.clone(),
                    message: "hyperliquid timeout cancel missing network".to_string(),
                }
            })?)
            .map_err(|err| OrderExecutionError::ProviderRequestFailed {
                provider: operation.provider.clone(),
                message: format!("invalid hyperliquid timeout cancel network: {err}"),
            })?;
        let asset = operation
            .request
            .get("asset_index")
            .and_then(Value::as_u64)
            .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                provider: operation.provider.clone(),
                message: "hyperliquid timeout cancel missing asset index".to_string(),
            })
            .and_then(|asset| {
                u32::try_from(asset).map_err(|err| OrderExecutionError::ProviderRequestFailed {
                    provider: operation.provider.clone(),
                    message: format!("invalid hyperliquid timeout cancel asset index: {err}"),
                })
            })?;
        let oid = operation
            .provider_ref
            .as_deref()
            .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                provider: operation.provider.clone(),
                message: "hyperliquid timeout cancel missing provider ref".to_string(),
            })
            .and_then(|raw| {
                raw.parse::<u64>()
                    .map_err(|err| OrderExecutionError::ProviderRequestFailed {
                        provider: operation.provider.clone(),
                        message: format!("invalid hyperliquid timeout cancel oid: {err}"),
                    })
            })?;
        let cancel_action = CustodyAction::Call(ChainCall::Hyperliquid(HyperliquidCall {
            target_base_url: target_base_url.to_string(),
            network,
            vault_address: None,
            payload: HyperliquidCallPayload::L1Action {
                action: HyperliquidActions::Cancel(BulkCancel {
                    cancels: vec![HyperliquidCancelRequest { asset, oid }],
                }),
            },
        }));
        custody_action_executor
            .execute(CustodyActionRequest {
                custody_vault_id,
                action: cancel_action,
            })
            .await
            .map_err(|source| OrderExecutionError::CustodyAction { source })?;
        Ok(())
    }

    pub async fn process_provider_operation_hints(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<usize> {
        let hints = self
            .db
            .orders()
            .claim_pending_provider_operation_hints(limit, Utc::now())
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut processed = 0_usize;

        for hint in hints {
            let disposition = self.process_provider_operation_hint(&hint).await;
            match disposition {
                Ok(HintDisposition::Processed) => {
                    self.db
                        .orders()
                        .complete_provider_operation_hint(
                            hint.id,
                            ProviderOperationHintStatus::Processed,
                            json!({}),
                            Utc::now(),
                        )
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                    processed += 1;
                }
                Ok(HintDisposition::Ignored { reason }) => {
                    self.db
                        .orders()
                        .complete_provider_operation_hint(
                            hint.id,
                            ProviderOperationHintStatus::Ignored,
                            json!({ "reason": reason }),
                            Utc::now(),
                        )
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                    processed += 1;
                }
                Err(error) => {
                    self.db
                        .orders()
                        .complete_provider_operation_hint(
                            hint.id,
                            ProviderOperationHintStatus::Failed,
                            json!({ "error": error.to_string() }),
                            Utc::now(),
                        )
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                    processed += 1;
                }
            }
        }

        Ok(processed)
    }

    async fn process_provider_operation_hint(
        &self,
        hint: &OrderProviderOperationHint,
    ) -> OrderExecutionResult<HintDisposition> {
        if hint.hint_kind != ProviderOperationHintKind::PossibleProgress {
            return Ok(HintDisposition::Ignored {
                reason: format!("unsupported hint kind {}", hint.hint_kind.to_db_string()),
            });
        }

        let operation = self
            .db
            .orders()
            .get_provider_operation(hint.provider_operation_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        match operation.status {
            ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired => {
                return Ok(HintDisposition::Ignored {
                    reason: format!(
                        "provider operation already terminal: {}",
                        operation.status.to_db_string()
                    ),
                });
            }
            ProviderOperationStatus::Planned
            | ProviderOperationStatus::Submitted
            | ProviderOperationStatus::WaitingExternal => {}
        }

        let Some(verifier) = self.verifier_for_operation(operation.operation_type) else {
            return Ok(HintDisposition::Ignored {
                reason: format!(
                    "provider operation type {} does not have hint validation yet",
                    operation.operation_type.to_db_string()
                ),
            });
        };
        let verification = verifier.verify(self, hint, &operation).await?;
        match verification {
            ProviderOperationHintVerification::StatusUpdate(update) => {
                self.apply_provider_operation_status_update(update).await?;
                Ok(HintDisposition::Processed)
            }
        }
    }

    fn verifier_for_operation(
        &self,
        operation_type: ProviderOperationType,
    ) -> Option<&'static dyn ProviderOperationVerifier> {
        match operation_type {
            ProviderOperationType::UnitDeposit => Some(&UNIT_DEPOSIT_VERIFIER),
            ProviderOperationType::AcrossBridge
            | ProviderOperationType::HyperliquidBridgeDeposit
            | ProviderOperationType::UnitWithdrawal
            | ProviderOperationType::HyperliquidTrade
            | ProviderOperationType::UniversalRouterSwap => Some(&PROVIDER_STATUS_VERIFIER),
        }
    }

    async fn verify_unit_deposit_hint(
        &self,
        hint: &OrderProviderOperationHint,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<ProviderOperationHintVerification> {
        if !hint.evidence.get("tx_hash").is_some()
            || !hint.evidence.get("address").is_some()
            || !hint.evidence.get("transfer_index").is_some()
        {
            return self.verify_provider_status_hint(hint, operation).await;
        }

        let tx_hash = required_evidence_str(hint, "tx_hash")?;
        let observed_address = required_evidence_str(hint, "address")?;
        let transfer_index = evidence_u64(hint, "transfer_index")?;
        let addresses = self
            .db
            .orders()
            .get_provider_addresses_by_operation(operation.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let Some(provider_address) = addresses.iter().find(|address| {
            address.role == ProviderAddressRole::UnitDeposit
                && address_matches(&address.address, observed_address)
        }) else {
            return Err(OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: format!(
                    "evidence address {observed_address} does not match a unit_deposit provider address"
                ),
            });
        };
        let verified_amount = self
            .verify_deposit_candidate(hint, provider_address, tx_hash, transfer_index)
            .await?;
        let observed_amount = match verified_amount {
            Some(amount) => amount,
            None => evidence_amount(hint, "amount")?,
        };

        let step = match operation.execution_step_id {
            Some(step_id) => Some(
                self.db
                    .orders()
                    .get_execution_step(step_id)
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?,
            ),
            None => None,
        };
        let expected_amount = expected_operation_amount(hint.id, operation, step.as_ref())?;
        if observed_amount < expected_amount {
            return Err(OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: format!(
                    "observed amount {observed_amount} is below expected amount {expected_amount}"
                ),
            });
        }

        let provider = self
            .action_providers
            .unit(&operation.provider)
            .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                provider: operation.provider.clone(),
                message: "unit provider is not configured".to_string(),
            })?;
        let request = ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: hint.evidence.clone(),
        };
        let provider_observation =
            provider
                .observe_unit_operation(request)
                .await
                .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                    provider: operation.provider.clone(),
                    message,
                })?;
        let (status, provider_observed_state, provider_tx_hash, provider_error) =
            if let Some(observation) = provider_observation {
                (
                    observation.status,
                    json_object_or_wrapped(observation.observed_state),
                    observation.tx_hash,
                    observation.error,
                )
            } else {
                (
                    ProviderOperationStatus::WaitingExternal,
                    json!({}),
                    None,
                    None,
                )
            };

        Ok(ProviderOperationHintVerification::StatusUpdate(
            ProviderOperationStatusUpdate {
                provider_operation_id: Some(operation.id),
                provider: Some(operation.provider.clone()),
                provider_ref: operation.provider_ref.clone(),
                status,
                observed_state: json!({
                    "source": "router_worker_hint_validation",
                    "hint_id": hint.id,
                    "hint_source": &hint.source,
                    "evidence": &hint.evidence,
                    "validated_provider_address_id": provider_address.id,
                    "expected_amount": expected_amount.to_string(),
                    "observed_amount": observed_amount.to_string(),
                    "transfer_index": transfer_index,
                    "chain_verified": verified_amount.is_some(),
                    "provider_observed_state": provider_observed_state,
                }),
                response: Some(json!({
                    "kind": "provider_operation_hint_validation",
                    "provider": &operation.provider,
                    "operation_id": operation.id,
                    "hint_id": hint.id,
                    "tx_hash": tx_hash,
                    "amount": observed_amount.to_string(),
                    "chain_verified": verified_amount.is_some(),
                })),
                tx_hash: Some(provider_tx_hash.unwrap_or_else(|| tx_hash.to_string())),
                error: provider_error,
            },
        ))
    }

    async fn verify_provider_status_hint(
        &self,
        hint: &OrderProviderOperationHint,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<ProviderOperationHintVerification> {
        let request = ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: hint.evidence.clone(),
        };
        let observation = match operation.operation_type {
            ProviderOperationType::AcrossBridge
            | ProviderOperationType::HyperliquidBridgeDeposit => {
                let provider = self
                    .action_providers
                    .bridge(&operation.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: operation.provider.clone(),
                        message: "bridge provider is not configured".to_string(),
                    })?;
                provider
                    .observe_bridge_operation(request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: operation.provider.clone(),
                        message,
                    })?
            }
            ProviderOperationType::UnitDeposit | ProviderOperationType::UnitWithdrawal => {
                let provider =
                    self.action_providers
                        .unit(&operation.provider)
                        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                            provider: operation.provider.clone(),
                            message: "unit provider is not configured".to_string(),
                        })?;
                provider
                    .observe_unit_operation(request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: operation.provider.clone(),
                        message,
                    })?
            }
            ProviderOperationType::HyperliquidTrade
            | ProviderOperationType::UniversalRouterSwap => {
                let provider = self
                    .action_providers
                    .exchange(&operation.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: operation.provider.clone(),
                        message: "exchange provider is not configured".to_string(),
                    })?;
                provider
                    .observe_trade_operation(request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: operation.provider.clone(),
                        message,
                    })?
            }
        };
        let observation =
            observation.ok_or_else(|| OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: format!(
                    "provider {} did not return an observation for {}",
                    operation.provider,
                    operation.operation_type.to_db_string()
                ),
            })?;
        if let Some(expected) = operation.provider_ref.as_deref() {
            let Some(observed) = observation.provider_ref.as_deref() else {
                return Err(OrderExecutionError::InvalidProviderOperationHint {
                    hint_id: hint.id,
                    reason: format!(
                        "provider observation for {} did not include expected operation ref {expected}",
                        operation.operation_type.to_db_string()
                    ),
                });
            };
            if expected != observed {
                return Err(OrderExecutionError::InvalidProviderOperationHint {
                    hint_id: hint.id,
                    reason: format!(
                        "provider observation ref {observed} does not match operation ref {expected}"
                    ),
                });
            }
        }

        Ok(ProviderOperationHintVerification::StatusUpdate(
            provider_status_observation_update(operation, hint, observation),
        ))
    }

    pub async fn process_planning_pass(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<Vec<OrderPlanMaterialization>> {
        let orders = self
            .db
            .orders()
            .get_market_orders_needing_execution_plan(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;

        let mut materialized = Vec::with_capacity(orders.len());
        for order in orders {
            match self.materialize_plan_for_order(order.id).await {
                Ok(result) => materialized.push(result),
                Err(OrderExecutionError::OrderNotReady { .. }) => {
                    // Another worker advanced the order past Funded between the
                    // fetch above and our CAS. Skip silently — it's benign.
                }
                Err(err @ OrderExecutionError::ProviderPolicyBlocked { .. }) => {
                    warn!(
                        order_id = %order.id,
                        error = %err,
                        "order planning blocked by provider policy; requesting refund",
                    );
                    if let Err(mark_err) = self.request_refund_for_unstarted_order(order.id).await {
                        warn!(
                            order_id = %order.id,
                            original_error = %err,
                            refund_error = %mark_err,
                            "failed to mark funded order refunding after provider policy block",
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        order_id = %order.id,
                        error = %err,
                        "order planning failed in worker pass",
                    );
                }
            }
        }

        Ok(materialized)
    }

    pub async fn materialize_plan_for_order(
        &self,
        order_id: Uuid,
    ) -> OrderExecutionResult<OrderPlanMaterialization> {
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let funding_vault_id = order
            .funding_vault_id
            .ok_or(OrderExecutionError::MissingFundingVault { order_id: order.id })?;
        let vault = self
            .db
            .vaults()
            .get(funding_vault_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if vault.status != DepositVaultStatus::Funded {
            return Err(OrderExecutionError::FundingVaultNotFunded { vault_id: vault.id });
        }
        let order = match order.status {
            RouterOrderStatus::PendingFunding => match self
                .db
                .orders()
                .transition_status(
                    order.id,
                    RouterOrderStatus::PendingFunding,
                    RouterOrderStatus::Funded,
                    Utc::now(),
                )
                .await
            {
                Ok(order) => order,
                Err(RouterServerError::NotFound) => {
                    // Another worker won the PendingFunding→Funded CAS. Re-read
                    // and proceed iff the order is now in a plannable state.
                    let refreshed = self
                        .db
                        .orders()
                        .get(order_id)
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                    match refreshed.status {
                        RouterOrderStatus::Funded => refreshed,
                        _ => return Err(OrderExecutionError::OrderNotReady { order_id }),
                    }
                }
                Err(source) => return Err(OrderExecutionError::Database { source }),
            },
            RouterOrderStatus::Funded => order,
            _ => {
                return Err(OrderExecutionError::OrderNotReady { order_id });
            }
        };
        let quote = self
            .db
            .orders()
            .get_market_order_quote(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let plan = self
            .market_order_planner
            .plan(&order, &vault, &quote, Utc::now())
            .map_err(|source| OrderExecutionError::RoutePlan { source })?;
        let plan_kind = plan.path_id.clone();
        self.enforce_route_provider_policy(&plan.steps, "planning")
            .await?;
        let mut steps = self.hydrate_planned_steps(order.id, plan.steps).await?;
        if self.custody_action_executor.is_some() {
            self.validate_materialized_intermediate_custody(order.id, &steps)
                .await?;
        }
        let execution_attempt = self.ensure_primary_execution_attempt(order.id).await?;
        self.bind_steps_to_attempt(
            order.id,
            execution_attempt.attempt_index,
            execution_attempt.id,
            &mut steps,
        );
        let planned_steps = steps.len();
        let inserted_steps = self
            .db
            .orders()
            .create_execution_steps_idempotent(&steps)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let persisted_steps = self
            .db
            .orders()
            .get_execution_steps_for_attempt(execution_attempt.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if persisted_steps.is_empty() {
            return Err(OrderExecutionError::ProviderRequestFailed {
                provider: "internal".to_string(),
                message: format!(
                    "execution attempt {} for order {} has no materialized execution steps",
                    execution_attempt.id, order.id
                ),
            });
        }
        if execution_attempt.status == OrderExecutionAttemptStatus::Planning {
            match self
                .db
                .orders()
                .transition_execution_attempt_status(
                    execution_attempt.id,
                    OrderExecutionAttemptStatus::Planning,
                    OrderExecutionAttemptStatus::Active,
                    Utc::now(),
                )
                .await
            {
                Ok(_) => {}
                Err(RouterServerError::NotFound) => {
                    let _ = self
                        .db
                        .orders()
                        .get_execution_attempt(execution_attempt.id)
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                }
                Err(source) => return Err(OrderExecutionError::Database { source }),
            }
        }

        Ok(OrderPlanMaterialization {
            order_id: order.id,
            plan_kind,
            planned_steps,
            inserted_steps,
        })
    }

    async fn materialize_refund_plan_for_order(
        &self,
        order_id: Uuid,
    ) -> OrderExecutionResult<bool> {
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if order.status != RouterOrderStatus::RefundRequired {
            return Ok(false);
        }
        let refund_attempt = self
            .db
            .orders()
            .get_latest_execution_attempt(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
            .ok_or(OrderExecutionError::OrderNotReady { order_id })?;
        if refund_attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery
            || refund_attempt.status != OrderExecutionAttemptStatus::RefundRequired
        {
            return Ok(false);
        }

        let positions = self.discover_refund_positions(&order).await?;
        if positions.len() != 1 {
            return self
                .mark_order_refund_manual_intervention(
                    &order,
                    refund_attempt.id,
                    json!({
                        "reason": "refund_requires_single_recoverable_position",
                        "position_count": positions.len(),
                    }),
                    refund_positions_snapshot(&positions),
                )
                .await;
        }
        let position = positions.into_iter().next().expect("checked len == 1");

        if let RefundSourceHandle::FundingVault(vault) = &position.handle {
            let _ = self
                .db
                .orders()
                .transition_execution_attempt_status(
                    refund_attempt.id,
                    OrderExecutionAttemptStatus::RefundRequired,
                    OrderExecutionAttemptStatus::Active,
                    Utc::now(),
                )
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            let _ = self
                .db
                .orders()
                .transition_status(
                    order.id,
                    RouterOrderStatus::RefundRequired,
                    RouterOrderStatus::Refunding,
                    Utc::now(),
                )
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            let _ = self
                .db
                .vaults()
                .request_refund(vault.id, Utc::now())
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            return Ok(true);
        }

        let mut steps = match self
            .build_automatic_refund_steps(&order, &position, refund_attempt.id)
            .await?
        {
            Some(steps) => steps,
            None => {
                return self
                    .mark_order_refund_manual_intervention(
                        &order,
                        refund_attempt.id,
                        json!({
                            "reason": "no_supported_automatic_refund_route",
                            "source_asset": {
                                "chain": position.asset.chain.as_str(),
                                "asset": position.asset.asset.as_str(),
                            },
                        }),
                        refund_positions_snapshot(std::slice::from_ref(&position)),
                    )
                    .await;
            }
        };
        self.bind_steps_to_attempt(
            order.id,
            refund_attempt.attempt_index,
            refund_attempt.id,
            &mut steps,
        );
        self.db
            .orders()
            .create_execution_steps_idempotent(&steps)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let _ = self
            .db
            .orders()
            .transition_execution_attempt_status(
                refund_attempt.id,
                OrderExecutionAttemptStatus::RefundRequired,
                OrderExecutionAttemptStatus::Active,
                Utc::now(),
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        Ok(true)
    }

    async fn discover_refund_positions(
        &self,
        order: &RouterOrder,
    ) -> OrderExecutionResult<Vec<RefundSourcePosition>> {
        let mut positions = Vec::new();
        if let Some(funding_vault_id) = order.funding_vault_id {
            let vault = self
                .db
                .vaults()
                .get(funding_vault_id)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            let amount = self.deposit_vault_balance_raw(&vault).await?;
            if decimal_string_positive(&amount) {
                positions.push(RefundSourcePosition {
                    handle: RefundSourceHandle::FundingVault(vault.clone()),
                    asset: vault.deposit_asset,
                    amount,
                });
            }
        }

        let custody_vaults = self
            .db
            .orders()
            .get_custody_vaults(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        for vault in custody_vaults {
            match vault.role {
                CustodyVaultRole::SourceDeposit => continue,
                CustodyVaultRole::HyperliquidSpot => {
                    let Some(executor) = self.custody_action_executor.as_ref() else {
                        continue;
                    };
                    let balances = executor
                        .inspect_hyperliquid_spot_balances(&vault)
                        .await
                        .map_err(|source| OrderExecutionError::CustodyAction { source })?;
                    for balance in balances.into_iter().filter(|balance| {
                        decimal_string_positive(&balance.total)
                            && !decimal_string_positive(&balance.hold)
                    }) {
                        let Some((canonical, asset)) = self
                            .action_providers
                            .asset_registry()
                            .hyperliquid_coin_asset(&balance.coin, Some(&order.source_asset.chain))
                        else {
                            continue;
                        };
                        positions.push(RefundSourcePosition {
                            handle: RefundSourceHandle::HyperliquidSpot {
                                vault: vault.clone(),
                                coin: balance.coin,
                                canonical,
                            },
                            asset: asset.clone(),
                            amount: decimal_to_raw_string(&balance.total, asset_decimals(&asset)),
                        });
                    }
                }
                _ => {
                    let Some(asset) = vault.asset.clone() else {
                        continue;
                    };
                    let deposit_asset = DepositAsset {
                        chain: vault.chain.clone(),
                        asset,
                    };
                    let amount = self.custody_vault_balance_raw(&vault).await?;
                    if decimal_string_positive(&amount) {
                        positions.push(RefundSourcePosition {
                            handle: RefundSourceHandle::ExternalCustody(vault),
                            asset: deposit_asset,
                            amount,
                        });
                    }
                }
            }
        }

        Ok(positions)
    }

    async fn deposit_vault_balance_raw(
        &self,
        vault: &DepositVault,
    ) -> OrderExecutionResult<String> {
        let chain_registry = self.chain_registry.as_ref().ok_or_else(|| {
            OrderExecutionError::ProviderRequestFailed {
                provider: "refund".to_string(),
                message: "chain registry is not configured".to_string(),
            }
        })?;
        let backend_chain = backend_chain_for_id(&vault.deposit_asset.chain).ok_or_else(|| {
            OrderExecutionError::ProviderRequestFailed {
                provider: "refund".to_string(),
                message: format!("unsupported chain {}", vault.deposit_asset.chain.as_str()),
            }
        })?;
        match backend_chain {
            router_primitives::ChainType::Bitcoin => {
                let chain = chain_registry.get_bitcoin(&backend_chain).ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: "bitcoin chain is not configured".to_string(),
                    }
                })?;
                Ok(chain
                    .address_balance_sats(&vault.deposit_vault_address)
                    .await
                    .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: source.to_string(),
                    })?
                    .to_string())
            }
            _ => {
                let chain = chain_registry.get_evm(&backend_chain).ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: format!(
                            "evm chain {} is not configured",
                            vault.deposit_asset.chain.as_str()
                        ),
                    }
                })?;
                match &vault.deposit_asset.asset {
                    AssetId::Native => Ok(chain
                        .native_balance(&vault.deposit_vault_address)
                        .await
                        .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                            provider: "refund".to_string(),
                            message: source.to_string(),
                        })?
                        .to_string()),
                    AssetId::Reference(token) => Ok(chain
                        .erc20_balance(token, &vault.deposit_vault_address)
                        .await
                        .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                            provider: "refund".to_string(),
                            message: source.to_string(),
                        })?
                        .to_string()),
                }
            }
        }
    }

    async fn custody_vault_balance_raw(
        &self,
        vault: &CustodyVault,
    ) -> OrderExecutionResult<String> {
        let Some(asset) = vault.asset.as_ref() else {
            return Ok("0".to_string());
        };
        let chain_registry = self.chain_registry.as_ref().ok_or_else(|| {
            OrderExecutionError::ProviderRequestFailed {
                provider: "refund".to_string(),
                message: "chain registry is not configured".to_string(),
            }
        })?;
        let backend_chain = backend_chain_for_id(&vault.chain).ok_or_else(|| {
            OrderExecutionError::ProviderRequestFailed {
                provider: "refund".to_string(),
                message: format!("unsupported chain {}", vault.chain.as_str()),
            }
        })?;
        match backend_chain {
            router_primitives::ChainType::Bitcoin => {
                let chain = chain_registry.get_bitcoin(&backend_chain).ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: "bitcoin chain is not configured".to_string(),
                    }
                })?;
                Ok(chain
                    .address_balance_sats(&vault.address)
                    .await
                    .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: source.to_string(),
                    })?
                    .to_string())
            }
            _ => {
                let chain = chain_registry.get_evm(&backend_chain).ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: format!("evm chain {} is not configured", vault.chain.as_str()),
                    }
                })?;
                match asset {
                    AssetId::Native => Ok(chain
                        .native_balance(&vault.address)
                        .await
                        .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                            provider: "refund".to_string(),
                            message: source.to_string(),
                        })?
                        .to_string()),
                    AssetId::Reference(token) => Ok(chain
                        .erc20_balance(token, &vault.address)
                        .await
                        .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                            provider: "refund".to_string(),
                            message: source.to_string(),
                        })?
                        .to_string()),
                }
            }
        }
    }

    async fn build_automatic_refund_steps(
        &self,
        order: &RouterOrder,
        position: &RefundSourcePosition,
        refund_attempt_id: Uuid,
    ) -> OrderExecutionResult<Option<Vec<OrderExecutionStep>>> {
        let planned_at = Utc::now();
        match &position.handle {
            RefundSourceHandle::ExternalCustody(vault) if position.asset == order.source_asset => {
                return Ok(Some(vec![refund_transfer_step(
                    order,
                    vault.id,
                    &position.amount,
                    1,
                    planned_at,
                )]));
            }
            RefundSourceHandle::FundingVault(_) => {}
            RefundSourceHandle::ExternalCustody(_) | RefundSourceHandle::HyperliquidSpot { .. } => {
                let Some(quoted_path) = self.best_refund_path_quote(order, position).await? else {
                    return Ok(None);
                };
                return Ok(Some(materialize_refund_transition_steps(
                    order,
                    position,
                    &quoted_path,
                    refund_attempt_id,
                    planned_at,
                )?));
            }
        }
        Ok(None)
    }

    async fn best_refund_path_quote(
        &self,
        order: &RouterOrder,
        position: &RefundSourcePosition,
    ) -> OrderExecutionResult<Option<RefundQuotedPath>> {
        let start = refund_position_start_node(position);
        let goal = MarketOrderNode::External(order.source_asset.clone());
        let mut paths = self
            .action_providers
            .asset_registry()
            .select_transition_paths_between(start, goal, REFUND_PATH_MAX_DEPTH);
        paths.retain(|path| refund_path_compatible_with_position(position, path));
        if paths.is_empty() {
            return Ok(None);
        }
        paths.sort_by_key(|path| path.transitions.len());
        paths.truncate(REFUND_TOP_K_PATHS);

        let provider_policy_snapshot = if let Some(service) = self.provider_policies.as_ref() {
            Some(
                service
                    .snapshot()
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?,
            )
        } else {
            None
        };

        let mut best_for_position: Option<RefundQuotedPath> = None;
        for path in paths {
            if !refund_path_providers_allowed(provider_policy_snapshot.as_ref(), &path) {
                continue;
            }
            let requires_unit = path.transitions.iter().any(|transition| {
                matches!(
                    transition.kind,
                    MarketOrderTransitionKind::UnitDeposit
                        | MarketOrderTransitionKind::UnitWithdrawal
                )
            });
            let requires_exchange = path.transitions.iter().any(|transition| {
                matches!(
                    transition.kind,
                    MarketOrderTransitionKind::HyperliquidTrade
                        | MarketOrderTransitionKind::UniversalRouterSwap
                )
            });
            let unit_candidates: Vec<Option<Arc<dyn UnitProvider>>> = if requires_unit {
                self.action_providers
                    .units()
                    .iter()
                    .filter(|unit| {
                        refund_provider_allowed_for_new_routes(
                            provider_policy_snapshot.as_ref(),
                            unit.id(),
                        ) && refund_unit_path_compatible(unit.as_ref(), &path)
                    })
                    .cloned()
                    .map(Some)
                    .collect()
            } else {
                vec![None]
            };
            if unit_candidates.is_empty() {
                continue;
            }

            let exchange_candidates: Vec<Option<Arc<dyn ExchangeProvider>>> = if requires_exchange {
                self.action_providers
                    .exchanges()
                    .iter()
                    .filter(|exchange| {
                        refund_provider_allowed_for_new_routes(
                            provider_policy_snapshot.as_ref(),
                            exchange.id(),
                        ) && refund_exchange_path_compatible(exchange.id(), &path)
                    })
                    .cloned()
                    .map(Some)
                    .collect()
            } else {
                vec![None]
            };
            if exchange_candidates.is_empty() {
                continue;
            }

            for unit in &unit_candidates {
                for exchange in &exchange_candidates {
                    match self
                        .compose_refund_transition_path_quote(
                            order,
                            position,
                            &path,
                            unit.as_deref(),
                            exchange.as_deref(),
                        )
                        .await
                    {
                        Ok(Some(candidate)) => {
                            best_for_position =
                                choose_better_refund_quote(candidate, best_for_position)?;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            warn!(
                                order_id = %order.id,
                                path_id = %path.id,
                                error = %err,
                                "refund transition-path quote failed",
                            );
                        }
                    }
                }
            }
        }

        Ok(best_for_position)
    }

    async fn compose_refund_transition_path_quote(
        &self,
        order: &RouterOrder,
        position: &RefundSourcePosition,
        path: &TransitionPath,
        unit: Option<&dyn UnitProvider>,
        exchange: Option<&dyn ExchangeProvider>,
    ) -> OrderExecutionResult<Option<RefundQuotedPath>> {
        let mut expires_at = Utc::now() + ChronoDuration::minutes(10);
        let mut cursor_amount = position.amount.clone();
        let mut legs_per_transition: Vec<Vec<Value>> = vec![Vec::new(); path.transitions.len()];
        let quote_depositor_address = refund_position_quote_address(position)?;

        for (index, transition) in path.transitions.iter().enumerate() {
            match transition.kind {
                MarketOrderTransitionKind::AcrossBridge
                | MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                    let bridge_id = transition.provider.as_str();
                    let bridge = self.action_providers.bridge(bridge_id).ok_or_else(|| {
                        OrderExecutionError::ProviderRequestFailed {
                            provider: bridge_id.to_string(),
                            message: "bridge provider is not configured".to_string(),
                        }
                    })?;
                    let quote = quote_refund_bridge(
                        bridge.as_ref(),
                        BridgeQuoteRequest {
                            source_asset: transition.input.asset.clone(),
                            destination_asset: transition.output.asset.clone(),
                            order_kind: MarketOrderKind::ExactIn {
                                amount_in: cursor_amount.clone(),
                                min_amount_out: "1".to_string(),
                            },
                            recipient_address: order.refund_address.clone(),
                            depositor_address: quote_depositor_address.clone(),
                            partial_fills_enabled: false,
                        },
                    )
                    .await?;
                    let Some(quote) = quote else {
                        return Ok(None);
                    };
                    expires_at = expires_at.min(quote.expires_at);
                    cursor_amount = quote.amount_out.clone();
                    legs_per_transition[index].push(refund_transition_leg_json(
                        &transition.id,
                        transition.kind,
                        bridge.id(),
                        &transition.input.asset,
                        &transition.output.asset,
                        &quote.amount_in,
                        &quote.amount_out,
                        quote.expires_at,
                        quote.provider_quote,
                    ));
                }
                MarketOrderTransitionKind::UnitDeposit => {
                    let unit = unit.ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: "unit".to_string(),
                        message: "unit provider is required for refund unit_deposit".to_string(),
                    })?;
                    legs_per_transition[index].push(refund_transition_leg_json(
                        &transition.id,
                        transition.kind,
                        unit.id(),
                        &transition.input.asset,
                        &transition.output.asset,
                        &cursor_amount,
                        &cursor_amount,
                        expires_at,
                        json!({}),
                    ));
                }
                MarketOrderTransitionKind::HyperliquidTrade => {
                    let exchange =
                        exchange.ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                            provider: "hyperliquid".to_string(),
                            message: "exchange provider is required for refund trade".to_string(),
                        })?;
                    let mut quote_amount_in = cursor_amount.clone();
                    if index > 0
                        && path.transitions[index - 1].kind
                            == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                    {
                        quote_amount_in = reserve_refund_hyperliquid_spot_send_quote_gas(
                            "refund.hyperliquid_trade.amount_in",
                            &quote_amount_in,
                        )?;
                    }
                    let exchange_quote = quote_refund_exchange(
                        exchange,
                        ExchangeQuoteRequest {
                            input_asset: transition.input.asset.clone(),
                            output_asset: transition.output.asset.clone(),
                            input_decimals: None,
                            output_decimals: None,
                            order_kind: MarketOrderKind::ExactIn {
                                amount_in: quote_amount_in,
                                min_amount_out: "1".to_string(),
                            },
                            sender_address: None,
                            recipient_address: order.refund_address.clone(),
                        },
                    )
                    .await?;
                    let Some(exchange_quote) = exchange_quote else {
                        return Ok(None);
                    };
                    expires_at = expires_at.min(exchange_quote.expires_at);
                    cursor_amount = exchange_quote.amount_out.clone();
                    legs_per_transition[index] = refund_exchange_quote_transition_legs(
                        &transition.id,
                        transition.kind,
                        exchange.id(),
                        &exchange_quote,
                    )?;
                }
                MarketOrderTransitionKind::UniversalRouterSwap => {
                    let exchange =
                        exchange.ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                            provider: "velora".to_string(),
                            message:
                                "exchange provider is required for refund universal_router_swap"
                                    .to_string(),
                        })?;
                    let input_decimals = self
                        .exchange_asset_decimals_for_refund(&transition.input.asset)
                        .await?;
                    let output_decimals = self
                        .exchange_asset_decimals_for_refund(&transition.output.asset)
                        .await?;
                    let exchange_quote = quote_refund_exchange(
                        exchange,
                        ExchangeQuoteRequest {
                            input_asset: transition.input.asset.clone(),
                            output_asset: transition.output.asset.clone(),
                            input_decimals,
                            output_decimals,
                            order_kind: MarketOrderKind::ExactIn {
                                amount_in: cursor_amount.clone(),
                                min_amount_out: "1".to_string(),
                            },
                            sender_address: Some(quote_depositor_address.clone()),
                            recipient_address: order.refund_address.clone(),
                        },
                    )
                    .await?;
                    let Some(exchange_quote) = exchange_quote else {
                        return Ok(None);
                    };
                    expires_at = expires_at.min(exchange_quote.expires_at);
                    cursor_amount = exchange_quote.amount_out.clone();
                    legs_per_transition[index] = refund_exchange_quote_transition_legs(
                        &transition.id,
                        transition.kind,
                        exchange.id(),
                        &exchange_quote,
                    )?;
                }
                MarketOrderTransitionKind::UnitWithdrawal => {
                    let unit = unit.ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: "unit".to_string(),
                        message: "unit provider is required for refund unit_withdrawal".to_string(),
                    })?;
                    legs_per_transition[index].push(refund_transition_leg_json(
                        &transition.id,
                        transition.kind,
                        unit.id(),
                        &transition.input.asset,
                        &transition.output.asset,
                        &cursor_amount,
                        &cursor_amount,
                        expires_at,
                        json!({
                            "recipient_address": order.refund_address,
                        }),
                    ));
                }
            }
        }

        Ok(Some(RefundQuotedPath {
            path: path.clone(),
            amount_out: cursor_amount,
            legs: flatten_refund_transition_legs(legs_per_transition),
        }))
    }

    async fn exchange_asset_decimals_for_refund(
        &self,
        asset: &DepositAsset,
    ) -> OrderExecutionResult<Option<u8>> {
        if let Some(chain_asset) = self.action_providers.asset_registry().chain_asset(asset) {
            return Ok(Some(chain_asset.decimals));
        }
        match &asset.asset {
            AssetId::Native => Ok(Some(18)),
            AssetId::Reference(token_address) => {
                let Some(backend_chain) = backend_chain_for_id(&asset.chain) else {
                    return Ok(None);
                };
                if !matches!(
                    backend_chain,
                    ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base
                ) {
                    return Ok(None);
                }
                let Some(chain_registry) = self.chain_registry.as_ref() else {
                    return Ok(None);
                };
                let evm_chain = chain_registry.get_evm(&backend_chain).ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: format!(
                            "no EVM chain implementation is configured for {}",
                            asset.chain
                        ),
                    }
                })?;
                evm_chain
                    .erc20_decimals(token_address)
                    .await
                    .map(Some)
                    .map_err(|err| OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: format!(
                            "failed to read token decimals for {} {}: {err}",
                            asset.chain, asset.asset
                        ),
                    })
            }
        }
    }

    async fn ensure_primary_execution_attempt(
        &self,
        order_id: Uuid,
    ) -> OrderExecutionResult<OrderExecutionAttempt> {
        if let Some(attempt) = self
            .db
            .orders()
            .get_active_execution_attempt(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
        {
            return Ok(attempt);
        }
        let now = Utc::now();
        let attempt = OrderExecutionAttempt {
            id: Uuid::now_v7(),
            order_id,
            attempt_index: 1,
            attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
            status: OrderExecutionAttemptStatus::Planning,
            trigger_step_id: None,
            trigger_provider_operation_id: None,
            failure_reason: json!({}),
            input_custody_snapshot: json!({
                "source_kind": "funding_vault",
            }),
            created_at: now,
            updated_at: now,
        };
        match self.db.orders().create_execution_attempt(&attempt).await {
            Ok(()) => Ok(attempt),
            Err(RouterServerError::DatabaseQuery { source }) if is_unique_violation(&source) => {
                self.db
                    .orders()
                    .get_active_execution_attempt(order_id)
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: "internal".to_string(),
                        message: format!(
                            "execution attempt unique violation for order {order_id} but no active attempt was found"
                        ),
                    })
            }
            Err(source) => Err(OrderExecutionError::Database { source }),
        }
    }

    fn bind_steps_to_attempt(
        &self,
        order_id: Uuid,
        attempt_index: i32,
        execution_attempt_id: Uuid,
        steps: &mut [OrderExecutionStep],
    ) {
        for step in steps {
            step.execution_attempt_id = Some(execution_attempt_id);
            step.idempotency_key = execution_attempt_idempotency_key(
                order_id,
                attempt_index,
                &step.provider,
                step.step_index,
            );
        }
    }

    async fn hydrate_planned_steps(
        &self,
        order_id: Uuid,
        steps: Vec<OrderExecutionStep>,
    ) -> OrderExecutionResult<Vec<OrderExecutionStep>> {
        if self.custody_action_executor.is_none() {
            return Ok(steps);
        }

        let mut destination_execution_vault: Option<CustodyVault> = None;
        let mut hyperliquid_spot_vault: Option<CustodyVault> = None;
        let mut hydrated = Vec::with_capacity(steps.len());
        for mut step in steps {
            if json_string_equals(
                &step.request,
                "recipient_custody_vault_role",
                CustodyVaultRole::DestinationExecution.to_db_string(),
            ) {
                let asset = step.output_asset.clone().ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "destination execution recipient requires step output_asset"
                            .to_string(),
                    }
                })?;
                let vault = self
                    .ensure_destination_execution_vault(
                        order_id,
                        &asset,
                        &mut destination_execution_vault,
                    )
                    .await?;
                if step.request.get("recipient").is_some() {
                    set_json_value(&mut step.request, "recipient", json!(vault.address));
                }
                if step.request.get("recipient_address").is_some() {
                    set_json_value(&mut step.request, "recipient_address", json!(vault.address));
                }
                set_json_value(
                    &mut step.request,
                    "recipient_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "destination_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "destination_custody_vault_address",
                    json!(vault.address),
                );
            }

            if json_string_equals(
                &step.request,
                "source_custody_vault_role",
                CustodyVaultRole::DestinationExecution.to_db_string(),
            ) {
                let asset = step.input_asset.clone().ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "destination execution source requires step input_asset"
                            .to_string(),
                    }
                })?;
                let vault = self
                    .ensure_destination_execution_vault(
                        order_id,
                        &asset,
                        &mut destination_execution_vault,
                    )
                    .await?;
                set_json_value(
                    &mut step.request,
                    "source_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "source_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "source_custody_vault_address",
                    json!(vault.address),
                );
                set_json_value(
                    &mut step.request,
                    "source_custody_vault_address",
                    json!(vault.address),
                );
            }

            if json_string_equals(
                &step.request,
                "depositor_custody_vault_role",
                CustodyVaultRole::DestinationExecution.to_db_string(),
            ) {
                let asset = step.input_asset.clone().ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "destination execution depositor requires step input_asset"
                            .to_string(),
                    }
                })?;
                let vault = self
                    .ensure_destination_execution_vault(
                        order_id,
                        &asset,
                        &mut destination_execution_vault,
                    )
                    .await?;
                set_json_value(
                    &mut step.request,
                    "depositor_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(&mut step.request, "depositor_address", json!(vault.address));
                set_json_value(
                    &mut step.details,
                    "depositor_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "depositor_custody_vault_address",
                    json!(vault.address),
                );
            }

            if json_string_equals(
                &step.request,
                "refund_custody_vault_role",
                CustodyVaultRole::DestinationExecution.to_db_string(),
            ) {
                let asset = step.input_asset.clone().ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "destination execution refund requires step input_asset"
                            .to_string(),
                    }
                })?;
                let vault = self
                    .ensure_destination_execution_vault(
                        order_id,
                        &asset,
                        &mut destination_execution_vault,
                    )
                    .await?;
                set_json_value(
                    &mut step.request,
                    "refund_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(&mut step.request, "refund_address", json!(vault.address));
                set_json_value(
                    &mut step.details,
                    "refund_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "refund_custody_vault_address",
                    json!(vault.address),
                );
            }

            if json_string_equals(
                &step.request,
                "hyperliquid_custody_vault_role",
                CustodyVaultRole::HyperliquidSpot.to_db_string(),
            ) {
                let vault = self
                    .ensure_hyperliquid_spot_vault(order_id, &mut hyperliquid_spot_vault)
                    .await?;
                set_json_value(
                    &mut step.request,
                    "hyperliquid_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.request,
                    "hyperliquid_custody_vault_address",
                    json!(vault.address),
                );
                set_json_value(
                    &mut step.details,
                    "hyperliquid_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "hyperliquid_custody_vault_address",
                    json!(vault.address),
                );
            }

            if json_string_equals(
                &step.request,
                "hyperliquid_custody_vault_role",
                CustodyVaultRole::DestinationExecution.to_db_string(),
            ) {
                let chain_id = step
                    .request
                    .get("hyperliquid_custody_vault_chain_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "destination execution hyperliquid signer missing chain id"
                            .to_string(),
                    })?;
                let asset_id = step
                    .request
                    .get("hyperliquid_custody_vault_asset_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "destination execution hyperliquid signer missing asset id"
                            .to_string(),
                    })?;
                let signer_asset = DepositAsset {
                    chain: ChainId::parse(chain_id).map_err(|err| {
                        OrderExecutionError::ProviderRequestFailed {
                            provider: step.provider.clone(),
                            message: format!(
                                "invalid destination execution hyperliquid signer chain id: {err}"
                            ),
                        }
                    })?,
                    asset: AssetId::parse(asset_id).map_err(|err| {
                        OrderExecutionError::ProviderRequestFailed {
                            provider: step.provider.clone(),
                            message: format!(
                                "invalid destination execution hyperliquid signer asset id: {err}"
                            ),
                        }
                    })?,
                };
                let vault = self
                    .ensure_destination_execution_vault(
                        order_id,
                        &signer_asset,
                        &mut destination_execution_vault,
                    )
                    .await?;
                set_json_value(
                    &mut step.request,
                    "hyperliquid_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.request,
                    "hyperliquid_custody_vault_address",
                    json!(vault.address),
                );
                set_json_value(
                    &mut step.details,
                    "hyperliquid_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "hyperliquid_custody_vault_address",
                    json!(vault.address),
                );
            }

            if json_string_equals(
                &step.request,
                "revert_custody_vault_role",
                CustodyVaultRole::DestinationExecution.to_db_string(),
            ) {
                let asset = step
                    .input_asset
                    .clone()
                    .or_else(|| step.output_asset.clone())
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "destination execution revert requires an execution asset"
                            .to_string(),
                    })?;
                let vault = self
                    .ensure_destination_execution_vault(
                        order_id,
                        &asset,
                        &mut destination_execution_vault,
                    )
                    .await?;
                set_json_value(
                    &mut step.request,
                    "revert_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "revert_custody_vault_id",
                    json!(vault.id),
                );
                set_json_value(
                    &mut step.details,
                    "revert_custody_vault_address",
                    json!(vault.address),
                );
            }

            hydrated.push(step);
        }

        Ok(hydrated)
    }

    async fn ensure_destination_execution_vault(
        &self,
        order_id: Uuid,
        asset: &DepositAsset,
        cache: &mut Option<CustodyVault>,
    ) -> OrderExecutionResult<CustodyVault> {
        if let Some(vault) = cache.as_ref().filter(|vault| {
            vault.chain == asset.chain
                && vault.asset.as_ref() == Some(&asset.asset)
                && vault.role == CustodyVaultRole::DestinationExecution
        }) {
            return Ok(vault.clone());
        }

        let find_existing = || async {
            let vaults = self
                .db
                .orders()
                .get_custody_vaults(order_id)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            Ok::<_, OrderExecutionError>(vaults.into_iter().find(|vault| {
                vault.role == CustodyVaultRole::DestinationExecution
                    && vault.chain == asset.chain
                    && vault.asset.as_ref() == Some(&asset.asset)
                    && vault.status != CustodyVaultStatus::Failed
            }))
        };

        if let Some(vault) = find_existing().await? {
            *cache = Some(vault.clone());
            return Ok(vault);
        }

        let custody_action_executor = self.custody_action_executor.as_ref().ok_or_else(|| {
            OrderExecutionError::CustodyExecutorUnavailable {
                provider: "destination_execution_vault".to_string(),
            }
        })?;
        let create_result = custody_action_executor
            .create_router_derived_vault(
                order_id,
                CustodyVaultRole::DestinationExecution,
                CustodyVaultVisibility::Internal,
                asset.chain.clone(),
                Some(asset.asset.clone()),
                json!({ "source": "order_execution_plan_hydration" }),
            )
            .await;
        let vault = match create_result {
            Ok(vault) => vault,
            Err(CustodyActionError::Database {
                source: RouterServerError::DatabaseQuery { source },
            }) if is_unique_violation(&source) => {
                // Another worker raced and won the partial unique index on
                // (order_id, role, chain_id, asset_id). Re-read the winner's vault.
                find_existing().await?.ok_or_else(|| {
                    OrderExecutionError::ProviderRequestFailed {
                        provider: "destination_execution_vault".to_string(),
                        message: format!(
                            "custody vault unique violation for order {order_id} on chain {} asset {} but re-read returned none",
                            asset.chain.as_str(),
                            asset.asset.as_str(),
                        ),
                    }
                })?
            }
            Err(source) => return Err(OrderExecutionError::CustodyAction { source }),
        };
        *cache = Some(vault.clone());
        Ok(vault)
    }

    async fn validate_materialized_intermediate_custody(
        &self,
        order_id: Uuid,
        steps: &[OrderExecutionStep],
    ) -> OrderExecutionResult<()> {
        let execution_steps: Vec<&OrderExecutionStep> =
            steps.iter().filter(|step| step.step_index > 0).collect();
        if execution_steps.is_empty() {
            return Ok(());
        }

        let vaults = self
            .db
            .orders()
            .get_custody_vaults(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let vaults_by_id: HashMap<Uuid, &CustodyVault> =
            vaults.iter().map(|vault| (vault.id, vault)).collect();

        for (index, step) in execution_steps.iter().enumerate() {
            let is_final = index + 1 == execution_steps.len();
            validate_materialized_intermediate_custody_step(
                order_id,
                step,
                is_final,
                &vaults_by_id,
            )?;
        }

        Ok(())
    }

    async fn enforce_route_provider_policy(
        &self,
        steps: &[OrderExecutionStep],
        phase: &'static str,
    ) -> OrderExecutionResult<()> {
        let Some(service) = self.provider_policies.as_ref() else {
            return Ok(());
        };
        let snapshot = service
            .snapshot()
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut seen = std::collections::BTreeSet::new();
        for step in steps.iter().filter(|step| step.step_index > 0) {
            if !seen.insert(step.provider.as_str()) {
                continue;
            }
            let policy = snapshot.policy(&step.provider);
            if policy.execution_state.allows_new_execution() {
                continue;
            }
            let reason = if policy.reason.is_empty() {
                "provider_policy".to_string()
            } else {
                policy.reason.clone()
            };
            telemetry::record_provider_execution_blocked(
                &step.provider,
                policy.execution_state.to_db_string(),
                &reason,
            );
            return Err(OrderExecutionError::ProviderPolicyBlocked {
                provider: step.provider.clone(),
                phase,
                state: policy.execution_state.to_db_string().to_string(),
                reason,
            });
        }
        Ok(())
    }

    async fn enforce_provider_execution_policy(
        &self,
        provider: &str,
        phase: &'static str,
    ) -> OrderExecutionResult<()> {
        let Some(service) = self.provider_policies.as_ref() else {
            return Ok(());
        };
        let snapshot = service
            .snapshot()
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let policy = snapshot.policy(provider);
        if policy.execution_state.allows_new_execution() {
            return Ok(());
        }
        let reason = if policy.reason.is_empty() {
            "provider_policy".to_string()
        } else {
            policy.reason.clone()
        };
        telemetry::record_provider_execution_blocked(
            provider,
            policy.execution_state.to_db_string(),
            &reason,
        );
        Err(OrderExecutionError::ProviderPolicyBlocked {
            provider: provider.to_string(),
            phase,
            state: policy.execution_state.to_db_string().to_string(),
            reason,
        })
    }

    /// Hyperliquid execution identity used by UnitDeposit (destination) and
    /// UnitWithdrawal / HyperliquidTrade steps. This is always a per-order
    /// router-derived identity; shared configured Hyperliquid accounts are not
    /// allowed in routed execution.
    async fn ensure_hyperliquid_spot_vault(
        &self,
        order_id: Uuid,
        cache: &mut Option<CustodyVault>,
    ) -> OrderExecutionResult<CustodyVault> {
        if let Some(vault) = cache
            .as_ref()
            .filter(|vault| vault.role == CustodyVaultRole::HyperliquidSpot)
        {
            return Ok(vault.clone());
        }

        let find_existing = || async {
            let vaults = self
                .db
                .orders()
                .get_custody_vaults(order_id)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            Ok::<_, OrderExecutionError>(vaults.into_iter().find(|vault| {
                vault.role == CustodyVaultRole::HyperliquidSpot
                    && vault.status != CustodyVaultStatus::Failed
            }))
        };

        if let Some(vault) = find_existing().await? {
            *cache = Some(vault.clone());
            return Ok(vault);
        }

        let custody_action_executor = self.custody_action_executor.as_ref().ok_or_else(|| {
            OrderExecutionError::CustodyExecutorUnavailable {
                provider: "hyperliquid_spot_vault".to_string(),
            }
        })?;
        let hyperliquid_chain = ChainId::parse("hyperliquid").expect("valid router chain id");
        let create_result = custody_action_executor
            .create_router_derived_vault(
                order_id,
                CustodyVaultRole::HyperliquidSpot,
                CustodyVaultVisibility::Internal,
                hyperliquid_chain,
                None,
                json!({ "source": "order_execution_plan_hydration" }),
            )
            .await;
        let vault = match create_result {
            Ok(vault) => vault,
            Err(CustodyActionError::Database {
                source: RouterServerError::DatabaseQuery { source },
            }) if is_unique_violation(&source) => find_existing().await?.ok_or_else(|| {
                OrderExecutionError::ProviderRequestFailed {
                    provider: "hyperliquid_spot_vault".to_string(),
                    message: format!(
                        "custody vault unique violation for order {order_id} hyperliquid_spot but re-read returned none",
                    ),
                }
            })?,
            Err(source) => return Err(OrderExecutionError::CustodyAction { source }),
        };
        *cache = Some(vault.clone());
        Ok(vault)
    }

    /// Drive one materialized order forward.
    ///
    /// Returns `Ok(Some(summary))` when this call performed visible work on the
    /// order (won any state-transition CAS). Returns `Ok(None)` when the call
    /// was a pure no-op because another worker was driving the order or the
    /// order had already reached a terminal state. The Option distinction lets
    /// concurrent worker passes report only the worker that actually moved the
    /// order, not racing no-op callers.
    pub async fn execute_materialized_order(
        &self,
        order_id: Uuid,
    ) -> OrderExecutionResult<Option<OrderExecutionSummary>> {
        let mut progressed = false;
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let funding_vault_id = order
            .funding_vault_id
            .ok_or(OrderExecutionError::MissingFundingVault { order_id: order.id })?;
        let vault = self
            .db
            .vaults()
            .get(funding_vault_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if vault.status == DepositVaultStatus::Funded {
            match self
                .db
                .vaults()
                .transition_status(
                    vault.id,
                    DepositVaultStatus::Funded,
                    DepositVaultStatus::Executing,
                    Utc::now(),
                )
                .await
            {
                Ok(_) => progressed = true,
                Err(RouterServerError::NotFound) => {}
                Err(source) => return Err(OrderExecutionError::Database { source }),
            }
        }
        if vault.status == DepositVaultStatus::RefundRequired {
            match self
                .db
                .vaults()
                .transition_status(
                    vault.id,
                    DepositVaultStatus::RefundRequired,
                    DepositVaultStatus::Refunding,
                    Utc::now(),
                )
                .await
            {
                Ok(_) => progressed = true,
                Err(RouterServerError::NotFound) => {}
                Err(source) => return Err(OrderExecutionError::Database { source }),
            }
        }
        let order = match order.status {
            RouterOrderStatus::Funded => match self
                .db
                .orders()
                .transition_status(
                    order.id,
                    RouterOrderStatus::Funded,
                    RouterOrderStatus::Executing,
                    Utc::now(),
                )
                .await
            {
                Ok(order) => {
                    progressed = true;
                    order
                }
                Err(RouterServerError::NotFound) => return Ok(None),
                Err(source) => return Err(OrderExecutionError::Database { source }),
            },
            RouterOrderStatus::RefundRequired => match self
                .db
                .orders()
                .transition_status(
                    order.id,
                    RouterOrderStatus::RefundRequired,
                    RouterOrderStatus::Refunding,
                    Utc::now(),
                )
                .await
            {
                Ok(order) => {
                    progressed = true;
                    order
                }
                Err(RouterServerError::NotFound) => return Ok(None),
                Err(source) => return Err(OrderExecutionError::Database { source }),
            },
            RouterOrderStatus::Executing => order,
            RouterOrderStatus::Refunding => order,
            RouterOrderStatus::Completed
            | RouterOrderStatus::Refunded
            | RouterOrderStatus::RefundManualInterventionRequired
            | RouterOrderStatus::Failed
            | RouterOrderStatus::Expired => return Ok(None),
            RouterOrderStatus::Quoted | RouterOrderStatus::PendingFunding => {
                return Err(OrderExecutionError::OrderNotReady { order_id });
            }
        };

        let mut completed_steps = 0_usize;
        let execution_attempt = self
            .db
            .orders()
            .get_active_execution_attempt(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
            .ok_or(OrderExecutionError::OrderNotReady { order_id })?;
        let steps = self
            .db
            .orders()
            .get_execution_steps_for_attempt(execution_attempt.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if !steps.iter().any(|step| step.step_index > 0) {
            return Err(OrderExecutionError::OrderNotReady { order_id });
        }
        self.validate_materialized_intermediate_custody(order.id, &steps)
            .await?;
        for step in steps.into_iter().filter(|step| step.step_index > 0) {
            match step.status {
                OrderExecutionStepStatus::Completed => {
                    completed_steps += 1;
                    continue;
                }
                OrderExecutionStepStatus::Waiting | OrderExecutionStepStatus::Running => {
                    return Ok(finalize_summary(progressed, order.id, completed_steps));
                }
                OrderExecutionStepStatus::Planned | OrderExecutionStepStatus::Ready => {}
                OrderExecutionStepStatus::Failed
                | OrderExecutionStepStatus::Skipped
                | OrderExecutionStepStatus::Cancelled => {
                    return Ok(finalize_summary(progressed, order.id, completed_steps));
                }
            }

            match self.execute_step(step).await {
                Ok(StepExecutionOutcome::Completed) => {
                    progressed = true;
                    completed_steps += 1;
                }
                Ok(StepExecutionOutcome::Waiting) => {
                    progressed = true;
                    return Ok(finalize_summary(progressed, order.id, completed_steps));
                }
                Ok(StepExecutionOutcome::Skipped) => {
                    return Ok(finalize_summary(progressed, order.id, completed_steps));
                }
                Err(err) => {
                    if let Err(reconcile_err) =
                        self.process_failed_attempt_for_order(order.id).await
                    {
                        warn!(
                            order_id = %order.id,
                            original_error = %err,
                            reconcile_error = %reconcile_err,
                            "inline retry/refund decision failed after step error; backstop pass will retry",
                        );
                    }
                    return Err(err);
                }
            }
        }

        match self
            .db
            .vaults()
            .transition_status(
                funding_vault_id,
                DepositVaultStatus::Executing,
                DepositVaultStatus::Completed,
                Utc::now(),
            )
            .await
        {
            Ok(_) => progressed = true,
            Err(RouterServerError::NotFound) => {}
            Err(source) => return Err(OrderExecutionError::Database { source }),
        }
        match self
            .db
            .orders()
            .transition_status(
                order.id,
                RouterOrderStatus::Executing,
                RouterOrderStatus::Completed,
                Utc::now(),
            )
            .await
        {
            Ok(_) => progressed = true,
            Err(RouterServerError::NotFound) => {}
            Err(source) => return Err(OrderExecutionError::Database { source }),
        }
        let completed_order = self
            .db
            .orders()
            .get(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        match self
            .db
            .orders()
            .transition_execution_attempt_status(
                execution_attempt.id,
                OrderExecutionAttemptStatus::Active,
                OrderExecutionAttemptStatus::Completed,
                Utc::now(),
            )
            .await
        {
            Ok(_) | Err(RouterServerError::NotFound) => {}
            Err(source) => return Err(OrderExecutionError::Database { source }),
        }
        let _ = self
            .finalize_internal_custody_vaults_for_order(&completed_order)
            .await?;

        Ok(finalize_summary(progressed, order.id, completed_steps))
    }

    async fn request_refund_for_unstarted_order(&self, order_id: Uuid) -> OrderExecutionResult<()> {
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if order.status == RouterOrderStatus::Funded {
            let _ = self
                .db
                .orders()
                .transition_status(
                    order.id,
                    RouterOrderStatus::Funded,
                    RouterOrderStatus::Refunding,
                    Utc::now(),
                )
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
        }
        if let Some(funding_vault_id) = order.funding_vault_id {
            let _ = self
                .db
                .vaults()
                .request_refund(funding_vault_id, Utc::now())
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
        }
        Ok(())
    }

    async fn execute_step(
        &self,
        step: OrderExecutionStep,
    ) -> OrderExecutionResult<StepExecutionOutcome> {
        self.enforce_provider_execution_policy(step.provider.as_str(), "execution")
            .await?;
        let step_id = step.id;
        let running = match self
            .db
            .orders()
            .transition_execution_step_status(
                step.id,
                step.status,
                OrderExecutionStepStatus::Running,
                Utc::now(),
            )
            .await
        {
            Ok(running) => running,
            Err(RouterServerError::NotFound) => {
                // Another worker won the Ready/Planned → Running CAS. Bow out
                // cleanly so the caller can return a no-op summary without
                // misinterpreting this as a real failure.
                return Ok(StepExecutionOutcome::Skipped);
            }
            Err(source) => return Err(OrderExecutionError::Database { source }),
        };
        let completion = match self.execute_running_step(&running).await {
            Ok(completion) => completion,
            Err(err) => {
                if let Err(mark_err) = self
                    .db
                    .orders()
                    .fail_execution_step(
                        running.id,
                        json!({
                            "error": err.to_string()
                        }),
                        Utc::now(),
                    )
                    .await
                {
                    // Mark-failed losing its CAS (status != 'running') is benign —
                    // someone else drove the step to a terminal state. Any other
                    // DB error is surfaced so operators see the step wedged in
                    // Running rather than silently losing the original cause.
                    if !matches!(mark_err, RouterServerError::NotFound) {
                        warn!(
                            %step_id,
                            original_error = %err,
                            mark_failed_error = %mark_err,
                            "failed to mark step failed after execution error",
                        );
                    }
                }
                self.maybe_inject_crash(OrderExecutionCrashPoint::AfterStepMarkedFailed);
                return Err(err);
            }
        };

        match completion.outcome {
            RunningStepOutcome::Completed => {
                self.db
                    .orders()
                    .complete_execution_step(
                        running.id,
                        completion.response,
                        completion.tx_hash,
                        Utc::now(),
                    )
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?;
            }
            RunningStepOutcome::Waiting => {
                self.db
                    .orders()
                    .wait_execution_step(
                        running.id,
                        completion.response,
                        completion.tx_hash,
                        Utc::now(),
                    )
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?;
            }
        }
        self.maybe_inject_crash(OrderExecutionCrashPoint::AfterExecutionStepStatusPersisted);
        Ok(completion.outcome.into())
    }

    async fn execute_running_step(
        &self,
        running: &OrderExecutionStep,
    ) -> OrderExecutionResult<StepCompletion> {
        match running.step_type {
            OrderExecutionStepType::WaitForDeposit => {
                Err(OrderExecutionError::InternalStepNotExecutable {
                    step_type: "wait_for_deposit",
                })
            }
            OrderExecutionStepType::Refund => {
                let source_custody_vault_id = running
                    .request
                    .get("source_custody_vault_id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or(OrderExecutionError::StepNotExecutable {
                        step_id: running.id,
                    })?;
                let recipient_address = running
                    .request
                    .get("recipient_address")
                    .and_then(Value::as_str)
                    .ok_or(OrderExecutionError::StepNotExecutable {
                        step_id: running.id,
                    })?;
                let amount = running
                    .request
                    .get("amount")
                    .and_then(Value::as_str)
                    .ok_or(OrderExecutionError::StepNotExecutable {
                        step_id: running.id,
                    })?;
                let custody_action_executor =
                    self.custody_action_executor.clone().ok_or_else(|| {
                        OrderExecutionError::CustodyExecutorUnavailable {
                            provider: running.provider.clone(),
                        }
                    })?;
                let receipt = custody_action_executor
                    .execute(CustodyActionRequest {
                        custody_vault_id: source_custody_vault_id,
                        action: CustodyAction::Transfer {
                            to_address: recipient_address.to_string(),
                            amount: amount.to_string(),
                        },
                    })
                    .await
                    .map_err(|source| OrderExecutionError::CustodyAction { source })?;
                Ok(StepCompletion {
                    response: json!({
                        "kind": "refund_transfer",
                        "recipient_address": recipient_address,
                        "amount": amount,
                        "tx_hash": &receipt.tx_hash,
                    }),
                    tx_hash: Some(receipt.tx_hash),
                    outcome: RunningStepOutcome::Completed,
                })
            }
            OrderExecutionStepType::AcrossBridge => {
                let provider =
                    self.action_providers
                        .bridge(&running.provider)
                        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                            provider: running.provider.clone(),
                            message: "bridge provider is not configured".to_string(),
                        })?;
                let intent =
                    provider
                        .execute_bridge(&running.request)
                        .await
                        .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                            provider: running.provider.clone(),
                            message,
                        })?;
                self.prepare_provider_completion(running, intent).await
            }
            OrderExecutionStepType::HyperliquidBridgeDeposit => {
                let provider =
                    self.action_providers
                        .bridge(&running.provider)
                        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                            provider: running.provider.clone(),
                            message: "bridge provider is not configured".to_string(),
                        })?;
                let intent =
                    provider
                        .execute_bridge(&running.request)
                        .await
                        .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                            provider: running.provider.clone(),
                            message,
                        })?;
                self.prepare_provider_completion(running, intent).await
            }
            OrderExecutionStepType::UnitDeposit => {
                let provider = self
                    .action_providers
                    .unit(&running.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: running.provider.clone(),
                        message: "unit provider is not configured".to_string(),
                    })?;
                let intent =
                    provider
                        .execute_deposit(&running.request)
                        .await
                        .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                            provider: running.provider.clone(),
                            message,
                        })?;
                self.prepare_provider_completion(running, intent).await
            }
            OrderExecutionStepType::UnitWithdrawal => {
                let provider = self
                    .action_providers
                    .unit(&running.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: running.provider.clone(),
                        message: "unit provider is not configured".to_string(),
                    })?;
                let intent = provider
                    .execute_withdrawal(&running.request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: running.provider.clone(),
                        message,
                    })?;
                self.prepare_provider_completion(running, intent).await
            }
            OrderExecutionStepType::HyperliquidTrade => {
                let provider = self
                    .action_providers
                    .exchange(&running.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: running.provider.clone(),
                        message: "exchange provider is not configured".to_string(),
                    })?;
                let intent = provider
                    .execute_trade(&running.request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: running.provider.clone(),
                        message,
                    })?;
                self.prepare_provider_completion(running, intent).await
            }
            OrderExecutionStepType::UniversalRouterSwap => {
                let provider = self
                    .action_providers
                    .exchange(&running.provider)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: running.provider.clone(),
                        message: "exchange provider is not configured".to_string(),
                    })?;
                let intent = provider
                    .execute_trade(&running.request)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: running.provider.clone(),
                        message,
                    })?;
                self.prepare_provider_completion(running, intent).await
            }
        }
    }

    async fn prepare_provider_completion(
        &self,
        step: &OrderExecutionStep,
        intent: ProviderExecutionIntent,
    ) -> OrderExecutionResult<StepCompletion> {
        let retention_actions = self.retention_actions_for_step(step)?;
        match intent {
            ProviderExecutionIntent::ProviderOnly { response, state } => {
                if !retention_actions.is_empty() {
                    return Err(OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "retention actions require a custody-backed provider intent"
                            .to_string(),
                    });
                }
                self.persist_provider_state(step, &state, &response).await?;
                let outcome = provider_operation_outcome(&step.provider, state.operation.as_ref())?;
                Ok(StepCompletion {
                    response: json!({
                        "kind": "provider_only",
                        "provider": &step.provider,
                        "response": response,
                    }),
                    tx_hash: None,
                    outcome,
                })
            }
            ProviderExecutionIntent::CustodyAction {
                custody_vault_id,
                action,
                provider_context,
                state,
            } => {
                let custody_action_executor =
                    self.custody_action_executor.clone().ok_or_else(|| {
                        OrderExecutionError::CustodyExecutorUnavailable {
                            provider: step.provider.clone(),
                        }
                    })?;
                let mut balance_observation = self
                    .begin_step_balance_observation(step, custody_vault_id)
                    .await?;
                let mut retention_tx_hashes = Vec::with_capacity(retention_actions.len());
                for retention_action in retention_actions {
                    let retention_action =
                        retention_action.to_custody_action(Some(&action), &step.provider)?;
                    let receipt = custody_action_executor
                        .execute(CustodyActionRequest {
                            custody_vault_id,
                            action: retention_action,
                        })
                        .await
                        .map_err(|source| OrderExecutionError::CustodyAction { source })?;
                    retention_tx_hashes.push(receipt.tx_hash);
                }
                let action_for_response = action.clone();
                let receipt = custody_action_executor
                    .execute(CustodyActionRequest {
                        custody_vault_id,
                        action,
                    })
                    .await
                    .map_err(|source| OrderExecutionError::CustodyAction { source })?;
                balance_observation.capture_after(self).await?;
                let outcome = provider_operation_outcome(&step.provider, state.operation.as_ref())?;
                if outcome == RunningStepOutcome::Completed {
                    balance_observation.enforce_output_minimum(step, &step.provider)?;
                }
                let provider_response = json!({
                    "provider_context": provider_context.clone(),
                    "tx_hash": &receipt.tx_hash,
                    "balance_observation": balance_observation.to_json(),
                });
                self.persist_provider_state(step, &state, &provider_response)
                    .await?;
                Ok(StepCompletion {
                    response: json!({
                        "kind": "custody_action",
                        "provider": &step.provider,
                        "step_id": step.id,
                        "custody_vault_id": receipt.custody_vault_id,
                        "retention_tx_hashes": retention_tx_hashes,
                        "action": action_for_response,
                        "provider_context": provider_context,
                        "tx_hash": &receipt.tx_hash,
                        "balance_observation": balance_observation.to_json(),
                    }),
                    tx_hash: Some(receipt.tx_hash),
                    outcome,
                })
            }
            ProviderExecutionIntent::CustodyActions {
                custody_vault_id,
                actions,
                provider_context,
                mut state,
            } => {
                // Route post_execute to whichever provider category owns this
                // step — bridges (Across) and exchanges (Hyperliquid) both
                // emit `CustodyActions` intents now.
                let post_execute_provider: PostExecuteProvider = self
                    .action_providers
                    .bridge(&step.provider)
                    .map(PostExecuteProvider::Bridge)
                    .or_else(|| {
                        self.action_providers
                            .exchange(&step.provider)
                            .map(PostExecuteProvider::Exchange)
                    })
                    .or_else(|| {
                        self.action_providers
                            .unit(&step.provider)
                            .map(PostExecuteProvider::Unit)
                    })
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message:
                            "custody_actions intent requires a bridge, exchange, or unit provider"
                                .to_string(),
                    })?;
                let custody_action_executor =
                    self.custody_action_executor.clone().ok_or_else(|| {
                        OrderExecutionError::CustodyExecutorUnavailable {
                            provider: step.provider.clone(),
                        }
                    })?;
                let mut balance_observation = self
                    .begin_step_balance_observation(step, custody_vault_id)
                    .await?;
                let actions_for_response = actions.clone();
                let prepared_actions = prepare_custody_actions_with_retention(
                    actions,
                    retention_actions,
                    &step.provider,
                )?;
                let mut receipts = Vec::with_capacity(prepared_actions.provider_action_count);
                let mut retention_tx_hashes = Vec::new();
                for prepared in prepared_actions.actions {
                    let receipt = custody_action_executor
                        .execute(CustodyActionRequest {
                            custody_vault_id,
                            action: prepared.action,
                        })
                        .await
                        .map_err(|source| OrderExecutionError::CustodyAction { source })?;
                    if prepared.provider_action {
                        receipts.push(receipt);
                    } else {
                        retention_tx_hashes.push(receipt.tx_hash);
                    }
                }
                balance_observation.capture_after(self).await?;
                let patch = post_execute_provider
                    .post_execute(&provider_context, &receipts)
                    .await
                    .map_err(|message| OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message,
                    })?;
                apply_post_execute_patch(&mut state, patch, &step.provider)?;
                let tx_hashes: Vec<String> = receipts.iter().map(|r| r.tx_hash.clone()).collect();
                let primary_tx_hash = receipts.last().map(|r| r.tx_hash.clone());
                let outcome = provider_operation_outcome(&step.provider, state.operation.as_ref())?;
                if outcome == RunningStepOutcome::Completed {
                    balance_observation.enforce_output_minimum(step, &step.provider)?;
                }
                let provider_response = json!({
                    "provider_context": provider_context.clone(),
                    "tx_hashes": &tx_hashes,
                    "balance_observation": balance_observation.to_json(),
                });
                self.persist_provider_state(step, &state, &provider_response)
                    .await?;
                Ok(StepCompletion {
                    response: json!({
                        "kind": "custody_actions",
                        "provider": &step.provider,
                        "step_id": step.id,
                        "custody_vault_id": custody_vault_id,
                        "retention_tx_hashes": retention_tx_hashes,
                        "actions": actions_for_response,
                        "provider_context": provider_context,
                        "tx_hashes": tx_hashes,
                        "balance_observation": balance_observation.to_json(),
                    }),
                    tx_hash: primary_tx_hash,
                    outcome,
                })
            }
        }
    }

    async fn begin_step_balance_observation(
        &self,
        step: &OrderExecutionStep,
        source_custody_vault_id: Uuid,
    ) -> OrderExecutionResult<StepBalanceObservation> {
        let mut observation = StepBalanceObservation::for_step(step, source_custody_vault_id);
        observation.capture_before(self).await?;
        Ok(observation)
    }

    async fn observed_balance_for_asset(
        &self,
        asset: &DepositAsset,
        address: &str,
    ) -> OrderExecutionResult<Option<U256>> {
        let Some(chain_registry) = self.chain_registry.as_ref() else {
            return Ok(None);
        };
        let Some(backend_chain) = backend_chain_for_id(&asset.chain) else {
            return Ok(None);
        };
        match backend_chain {
            ChainType::Bitcoin => {
                if !asset.asset.is_native() {
                    return Ok(None);
                }
                let Some(chain) = chain_registry.get_bitcoin(&backend_chain) else {
                    return Err(OrderExecutionError::ProviderRequestFailed {
                        provider: "balance_observer".to_string(),
                        message: "bitcoin chain is not configured".to_string(),
                    });
                };
                chain
                    .address_balance_sats(address)
                    .await
                    .map(U256::from)
                    .map(Some)
                    .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                        provider: "balance_observer".to_string(),
                        message: source.to_string(),
                    })
            }
            ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base => {
                let Some(chain) = chain_registry.get_evm(&backend_chain) else {
                    return Err(OrderExecutionError::ProviderRequestFailed {
                        provider: "balance_observer".to_string(),
                        message: format!("evm chain {} is not configured", asset.chain.as_str()),
                    });
                };
                match &asset.asset {
                    AssetId::Native => {
                        chain
                            .native_balance(address)
                            .await
                            .map(Some)
                            .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                                provider: "balance_observer".to_string(),
                                message: source.to_string(),
                            })
                    }
                    AssetId::Reference(token) => chain
                        .erc20_balance(token, address)
                        .await
                        .map(Some)
                        .map_err(|source| OrderExecutionError::ProviderRequestFailed {
                            provider: "balance_observer".to_string(),
                            message: source.to_string(),
                        }),
                }
            }
            ChainType::Hyperliquid => Ok(None),
        }
    }

    fn retention_actions_for_step(
        &self,
        step: &OrderExecutionStep,
    ) -> OrderExecutionResult<Vec<PlannedRetentionAction>> {
        let Some(actions) = step
            .request
            .get("retention_actions")
            .and_then(Value::as_array)
        else {
            return Ok(vec![]);
        };
        let custody_action_executor = self.custody_action_executor.as_ref().ok_or_else(|| {
            OrderExecutionError::CustodyExecutorUnavailable {
                provider: step.provider.clone(),
            }
        })?;
        let mut custody_actions = Vec::with_capacity(actions.len());
        for action in actions {
            let recipient_role = action
                .get("recipient_role")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if recipient_role != "paymaster_wallet" {
                return Err(OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: format!("unsupported retention recipient role {recipient_role:?}"),
                });
            }
            let settlement_chain_id = action
                .get("settlement_chain_id")
                .and_then(Value::as_str)
                .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: "retention action missing settlement_chain_id".to_string(),
                })?;
            let settlement_asset_id = action
                .get("settlement_asset_id")
                .and_then(Value::as_str)
                .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: "retention action missing settlement_asset_id".to_string(),
                })?;
            let amount = action
                .get("amount")
                .and_then(Value::as_str)
                .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: "retention action missing amount".to_string(),
                })?;
            if amount == "0" || !amount.chars().all(|ch| ch.is_ascii_digit()) {
                return Err(OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: format!("invalid retention amount {amount:?}"),
                });
            }
            let raw_amount = U256::from_str_radix(amount, 10).map_err(|err| {
                OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: format!("invalid retention amount {amount:?}: {err}"),
                }
            })?;
            let settlement_decimals = action
                .get("settlement_decimals")
                .and_then(Value::as_u64)
                .and_then(|value| u8::try_from(value).ok())
                .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: "retention action missing settlement_decimals".to_string(),
                })?;
            let settlement_chain = ChainId::parse(settlement_chain_id).map_err(|err| {
                OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: format!("invalid retention settlement_chain_id: {err}"),
                }
            })?;
            let settlement_asset = AssetId::parse(settlement_asset_id).map_err(|err| {
                OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: format!("invalid retention settlement_asset_id: {err}"),
                }
            })?;
            if step.input_asset.as_ref()
                != Some(&DepositAsset {
                    chain: settlement_chain.clone(),
                    asset: settlement_asset,
                })
            {
                return Err(OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: "retention action settlement asset must match the step input asset"
                        .to_string(),
                });
            }
            let backend_chain = backend_chain_for_id(&settlement_chain).ok_or_else(|| {
                OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: format!(
                        "unsupported retention settlement chain {}",
                        settlement_chain
                    ),
                }
            })?;
            let to_address = custody_action_executor
                .paymaster_address(backend_chain)
                .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                    provider: step.provider.clone(),
                    message: format!(
                        "missing paymaster wallet for retention settlement chain {}",
                        settlement_chain
                    ),
                })?
                .to_string();
            if backend_chain == router_primitives::ChainType::Hyperliquid {
                let token = action
                    .get("settlement_provider_asset")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: step.provider.clone(),
                        message: "hyperliquid retention action missing settlement_provider_asset"
                            .to_string(),
                    })?
                    .to_string();
                custody_actions.push(PlannedRetentionAction::HyperliquidSpotSend {
                    destination: to_address,
                    token,
                    raw_amount,
                    decimals: settlement_decimals,
                });
            } else {
                custody_actions.push(PlannedRetentionAction::Transfer(CustodyAction::Transfer {
                    to_address,
                    amount: amount.to_string(),
                }));
            }
        }
        Ok(custody_actions)
    }

    async fn persist_provider_state(
        &self,
        step: &OrderExecutionStep,
        state: &ProviderExecutionState,
        fallback_response: &serde_json::Value,
    ) -> OrderExecutionResult<()> {
        let now = Utc::now();
        let provider_operation_id = if let Some(operation) = &state.operation {
            let operation_id = Uuid::now_v7();
            let operation_id = self
                .db
                .orders()
                .upsert_provider_operation(&OrderProviderOperation {
                    id: operation_id,
                    order_id: step.order_id,
                    execution_attempt_id: step.execution_attempt_id,
                    execution_step_id: Some(step.id),
                    provider: step.provider.clone(),
                    operation_type: operation.operation_type,
                    provider_ref: operation
                        .provider_ref
                        .clone()
                        .or_else(|| step.provider_ref.clone()),
                    status: operation.status,
                    request: json_object_or_wrapped(
                        operation
                            .request
                            .clone()
                            .unwrap_or_else(|| step.request.clone()),
                    ),
                    response: json_object_or_wrapped(
                        operation
                            .response
                            .clone()
                            .unwrap_or_else(|| fallback_response.clone()),
                    ),
                    observed_state: json_object_or_wrapped(
                        operation
                            .observed_state
                            .clone()
                            .unwrap_or_else(|| json!({})),
                    ),
                    created_at: now,
                    updated_at: now,
                })
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
            Some(operation_id)
        } else {
            None
        };

        for address in &state.addresses {
            self.db
                .orders()
                .upsert_provider_address(&OrderProviderAddress {
                    id: Uuid::now_v7(),
                    order_id: step.order_id,
                    execution_step_id: Some(step.id),
                    provider_operation_id,
                    provider: step.provider.clone(),
                    role: address.role,
                    chain: address.chain.clone(),
                    asset: address.asset.clone(),
                    address: address.address.clone(),
                    memo: address.memo.clone(),
                    expires_at: address.expires_at,
                    metadata: json_object_or_wrapped(
                        address.metadata.clone().unwrap_or_else(|| json!({})),
                    ),
                    created_at: now,
                    updated_at: now,
                })
                .await
                .map_err(|source| OrderExecutionError::Database { source })?;
        }

        Ok(())
    }

    pub async fn apply_provider_operation_status_update(
        &self,
        update: ProviderOperationStatusUpdate,
    ) -> OrderExecutionResult<ProviderOperationStatusUpdateOutcome> {
        let existing = self.load_status_update_provider_operation(&update).await?;
        let observed_state = json_object_or_wrapped(update.observed_state.clone());
        let response = update.response.clone().map(json_object_or_wrapped);
        let operation = self
            .db
            .orders()
            .update_provider_operation_status(
                existing.id,
                update.status,
                observed_state.clone(),
                response.clone(),
                Utc::now(),
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        self.maybe_inject_crash(OrderExecutionCrashPoint::AfterProviderOperationStatusPersisted);
        let step = self
            .apply_provider_status_update_to_step(
                &operation,
                observed_state,
                response,
                update.tx_hash,
                update.error,
            )
            .await?;
        self.maybe_inject_crash(OrderExecutionCrashPoint::AfterProviderStepSettlement);
        let order = self
            .settle_order_after_provider_status_update(&operation)
            .await?;

        Ok(ProviderOperationStatusUpdateOutcome {
            operation,
            step,
            order,
        })
    }

    fn maybe_inject_crash(&self, point: OrderExecutionCrashPoint) {
        self.crash_injector.trigger(point);
    }

    async fn load_status_update_provider_operation(
        &self,
        update: &ProviderOperationStatusUpdate,
    ) -> OrderExecutionResult<OrderProviderOperation> {
        let operation = if let Some(id) = update.provider_operation_id {
            self.db
                .orders()
                .get_provider_operation(id)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?
        } else {
            let provider = update
                .provider
                .as_deref()
                .ok_or(OrderExecutionError::MissingProviderStatusUpdateSelector)?;
            let provider_ref = update
                .provider_ref
                .as_deref()
                .ok_or(OrderExecutionError::MissingProviderStatusUpdateSelector)?;
            self.db
                .orders()
                .get_provider_operation_by_ref(provider, provider_ref)
                .await
                .map_err(|source| OrderExecutionError::Database { source })?
        };

        if let Some(provider) = &update.provider {
            if provider != &operation.provider {
                return Err(OrderExecutionError::ProviderStatusUpdateProviderMismatch {
                    operation_id: operation.id,
                    provider: provider.clone(),
                });
            }
        }

        Ok(operation)
    }

    async fn apply_provider_status_update_to_step(
        &self,
        operation: &OrderProviderOperation,
        observed_state: Value,
        response: Option<Value>,
        tx_hash: Option<String>,
        error: Option<Value>,
    ) -> OrderExecutionResult<Option<OrderExecutionStep>> {
        let Some(step_id) = operation.execution_step_id else {
            return Ok(None);
        };
        let step = self
            .db
            .orders()
            .get_execution_step(step_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;

        match operation.status {
            ProviderOperationStatus::Planned
            | ProviderOperationStatus::Submitted
            | ProviderOperationStatus::WaitingExternal => Ok(Some(step)),
            ProviderOperationStatus::Completed => {
                if step.status == OrderExecutionStepStatus::Completed {
                    return Ok(Some(step));
                }
                let response = response.unwrap_or_else(|| {
                    json!({
                        "kind": "provider_status_update",
                        "provider": &operation.provider,
                        "operation_id": operation.id,
                        "provider_ref": &operation.provider_ref,
                        "observed_state": observed_state,
                    })
                });
                let step = self
                    .db
                    .orders()
                    .complete_observed_execution_step(step.id, response, tx_hash, Utc::now())
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?;
                Ok(Some(step))
            }
            ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
                if step.status == OrderExecutionStepStatus::Failed {
                    return Ok(Some(step));
                }
                let error = error.unwrap_or_else(|| {
                    json!({
                        "kind": "provider_status_update",
                        "provider": &operation.provider,
                        "operation_id": operation.id,
                        "provider_ref": &operation.provider_ref,
                        "status": operation.status.to_db_string(),
                        "observed_state": observed_state,
                    })
                });
                let step = self
                    .db
                    .orders()
                    .fail_observed_execution_step(step.id, error, Utc::now())
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })?;
                Ok(Some(step))
            }
        }
    }

    async fn settle_order_after_provider_status_update(
        &self,
        operation: &OrderProviderOperation,
    ) -> OrderExecutionResult<RouterOrder> {
        let order = self
            .db
            .orders()
            .get(operation.order_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;

        match operation.status {
            ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
                let _ = self.process_failed_attempt_for_order(order.id).await?;
                self.db
                    .orders()
                    .get(order.id)
                    .await
                    .map_err(|source| OrderExecutionError::Database { source })
            }
            ProviderOperationStatus::Completed => self.finalize_order_if_complete(order).await,
            ProviderOperationStatus::Planned
            | ProviderOperationStatus::Submitted
            | ProviderOperationStatus::WaitingExternal => Ok(order),
        }
    }

    async fn finalize_order_if_complete(
        &self,
        order: RouterOrder,
    ) -> OrderExecutionResult<RouterOrder> {
        if !matches!(
            order.status,
            RouterOrderStatus::Executing | RouterOrderStatus::Refunding
        ) {
            return Ok(order);
        }
        let Some(active_attempt) = self
            .db
            .orders()
            .get_active_execution_attempt(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
        else {
            return Ok(order);
        };
        let steps = self
            .db
            .orders()
            .get_execution_steps_for_attempt(active_attempt.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let has_execution_steps = steps.iter().any(|step| step.step_index > 0);
        let all_execution_steps_completed = has_execution_steps
            && steps
                .iter()
                .filter(|step| step.step_index > 0)
                .all(|step| step.status == OrderExecutionStepStatus::Completed);
        if !all_execution_steps_completed {
            return Ok(order);
        }

        let (from_status, to_status, from_vault_status, to_vault_status) =
            if active_attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery {
                (
                    RouterOrderStatus::Refunding,
                    RouterOrderStatus::Refunded,
                    DepositVaultStatus::Refunding,
                    DepositVaultStatus::Refunded,
                )
            } else {
                (
                    RouterOrderStatus::Executing,
                    RouterOrderStatus::Completed,
                    DepositVaultStatus::Executing,
                    DepositVaultStatus::Completed,
                )
            };
        if let Some(funding_vault_id) = order.funding_vault_id {
            match self
                .db
                .vaults()
                .transition_status(
                    funding_vault_id,
                    from_vault_status,
                    to_vault_status,
                    Utc::now(),
                )
                .await
            {
                Ok(_) => {}
                Err(source @ RouterServerError::NotFound) => {
                    let current = self
                        .db
                        .vaults()
                        .get(funding_vault_id)
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                    if current.status != to_vault_status {
                        return Err(OrderExecutionError::Database { source });
                    }
                }
                Err(source) => return Err(OrderExecutionError::Database { source }),
            }
        }
        let order = self
            .db
            .orders()
            .transition_status(order.id, from_status, to_status, Utc::now())
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        match self
            .db
            .orders()
            .transition_execution_attempt_status(
                active_attempt.id,
                OrderExecutionAttemptStatus::Active,
                OrderExecutionAttemptStatus::Completed,
                Utc::now(),
            )
            .await
        {
            Ok(_) | Err(RouterServerError::NotFound) => {}
            Err(source) => return Err(OrderExecutionError::Database { source }),
        }
        let _ = self
            .finalize_internal_custody_vaults_for_order(&order)
            .await?;
        Ok(order)
    }

    async fn finalize_direct_refund_if_complete(
        &self,
        order: RouterOrder,
    ) -> OrderExecutionResult<bool> {
        if order.status != RouterOrderStatus::Refunding {
            return Ok(false);
        }
        let Some(active_attempt) = self
            .db
            .orders()
            .get_active_execution_attempt(order.id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?
        else {
            return Ok(false);
        };
        if active_attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery {
            return Ok(false);
        }

        let order = self
            .db
            .orders()
            .transition_status(
                order.id,
                RouterOrderStatus::Refunding,
                RouterOrderStatus::Refunded,
                Utc::now(),
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        self.db
            .orders()
            .transition_execution_attempt_status(
                active_attempt.id,
                OrderExecutionAttemptStatus::Active,
                OrderExecutionAttemptStatus::Completed,
                Utc::now(),
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let _ = self
            .finalize_internal_custody_vaults_for_order(&order)
            .await?;
        Ok(true)
    }

    async fn process_terminal_internal_custody_vaults(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<usize> {
        let orders = self
            .db
            .orders()
            .get_terminal_orders_with_pending_internal_custody_finalization(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut finalized = 0usize;
        for order in orders {
            finalized += self
                .finalize_internal_custody_vaults_for_order(&order)
                .await?;
        }
        Ok(finalized)
    }

    async fn process_released_internal_custody_sweeps(
        &self,
        limit: i64,
    ) -> OrderExecutionResult<usize> {
        let Some(custody_action_executor) = self.custody_action_executor.as_ref() else {
            return Ok(0);
        };
        let vaults = self
            .db
            .orders()
            .get_released_internal_custody_vaults_pending_sweep(limit)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        let mut processed = 0usize;
        for vault in vaults {
            match custody_action_executor
                .sweep_released_internal_custody(&vault)
                .await
            {
                Ok(result) => {
                    let now = Utc::now();
                    self.db
                        .orders()
                        .patch_custody_vault_metadata(
                            vault.id,
                            result.metadata_patch().clone(),
                            now,
                        )
                        .await
                        .map_err(|source| OrderExecutionError::Database { source })?;
                    if matches!(
                        result,
                        ReleasedSweepResult::Swept { .. } | ReleasedSweepResult::Skipped { .. }
                    ) {
                        processed += 1;
                    }
                }
                Err(err) => {
                    warn!(
                        custody_vault_id = %vault.id,
                        chain = %vault.chain,
                        address = %vault.address,
                        error = %err,
                        "released internal custody sweep failed",
                    );
                }
            }
        }
        Ok(processed)
    }

    async fn finalize_internal_custody_vaults_for_order(
        &self,
        order: &RouterOrder,
    ) -> OrderExecutionResult<usize> {
        let Some((status, reason)) = internal_custody_terminal_status(order.status) else {
            return Ok(0);
        };
        let finalized_at = Utc::now();
        let finalized = self
            .db
            .orders()
            .finalize_internal_custody_vaults(
                order.id,
                status,
                json!({
                    "lifecycle_terminal_reason": reason,
                    "lifecycle_order_status": order.status.to_db_string(),
                    "lifecycle_balance_verified": false,
                    "lifecycle_finalized_by": "order_execution_manager",
                    "lifecycle_finalized_at": finalized_at.to_rfc3339(),
                }),
                finalized_at,
            )
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        Ok(finalized.len())
    }

    async fn verify_deposit_candidate(
        &self,
        hint: &OrderProviderOperationHint,
        provider_address: &OrderProviderAddress,
        tx_hash: &str,
        transfer_index: u64,
    ) -> OrderExecutionResult<Option<U256>> {
        let Some(chain_registry) = &self.chain_registry else {
            return Ok(None);
        };
        let backend_chain = backend_chain_for_id(&provider_address.chain).ok_or_else(|| {
            OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: format!(
                    "provider address chain {} is not supported by the router worker",
                    provider_address.chain
                ),
            }
        })?;
        let chain = chain_registry.get(&backend_chain).ok_or_else(|| {
            OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: format!(
                    "router worker has no chain implementation for {}",
                    backend_chain.to_db_string()
                ),
            }
        })?;
        let asset = provider_address.asset.as_ref().ok_or_else(|| {
            OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: "provider address does not declare an asset".to_string(),
            }
        })?;
        let currency = Currency {
            chain: backend_chain,
            token: token_identifier(asset),
            decimals: currency_decimals(&backend_chain, asset),
        };

        match chain
            .verify_user_deposit_candidate(
                &provider_address.address,
                &currency,
                tx_hash,
                transfer_index,
            )
            .await
        {
            Ok(UserDepositCandidateStatus::Verified(deposit)) => Ok(Some(deposit.amount)),
            Ok(UserDepositCandidateStatus::TxNotFound) => {
                Err(OrderExecutionError::InvalidProviderOperationHint {
                    hint_id: hint.id,
                    reason: format!("candidate transaction {tx_hash} was not found"),
                })
            }
            Ok(UserDepositCandidateStatus::TransferNotFound) => {
                Err(OrderExecutionError::InvalidProviderOperationHint {
                    hint_id: hint.id,
                    reason: format!(
                        "candidate transaction {tx_hash} does not pay provider address {} at transfer index {transfer_index}",
                        provider_address.address
                    ),
                })
            }
            Err(err) => Err(OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: format!("failed to verify candidate deposit on chain: {err}"),
            }),
        }
    }
}

fn refund_transfer_step(
    order: &RouterOrder,
    source_custody_vault_id: Uuid,
    amount: &str,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionStep {
    OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        transition_decl_id: None,
        step_index,
        step_type: OrderExecutionStepType::Refund,
        provider: "internal".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: Some(order.source_asset.clone()),
        output_asset: Some(order.source_asset.clone()),
        amount_in: Some(amount.to_string()),
        min_amount_out: None,
        tx_hash: None,
        provider_ref: None,
        idempotency_key: None,
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: json!({
            "schema_version": 1,
            "refund_kind": "direct_transfer",
            "source_custody_vault_id": source_custody_vault_id,
            "recipient_address": &order.refund_address,
        }),
        request: json!({
            "order_id": order.id,
            "source_custody_vault_id": source_custody_vault_id,
            "recipient_address": &order.refund_address,
            "amount": amount,
        }),
        response: json!({}),
        error: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    }
}

#[derive(Debug, Clone)]
struct RefundQuotedPath {
    path: TransitionPath,
    amount_out: String,
    legs: Vec<Value>,
}

#[derive(Debug, Clone)]
struct RefundQuoteLegIndex {
    by_transition_id: HashMap<String, Vec<Value>>,
}

#[derive(Debug, Clone)]
enum RefundExternalSourceBinding {
    Explicit {
        vault_id: Uuid,
        address: String,
        role: CustodyVaultRole,
    },
    DerivedDestinationExecution,
}

#[derive(Debug, Clone)]
enum RefundHyperliquidBinding {
    Explicit {
        vault_id: Uuid,
        address: String,
        role: CustodyVaultRole,
        asset: Option<DepositAsset>,
    },
    DerivedSpot,
    DerivedDestinationExecution {
        asset: DepositAsset,
    },
}

impl RefundQuoteLegIndex {
    fn from_legs(legs: &[Value]) -> OrderExecutionResult<Self> {
        let mut by_transition_id: HashMap<String, Vec<Value>> = HashMap::new();
        for leg in legs {
            let transition_id = refund_leg_parent_transition_id(leg).ok_or_else(|| {
                OrderExecutionError::ProviderRequestFailed {
                    provider: "refund".to_string(),
                    message: format!(
                        "quoted refund leg is missing transition declaration metadata: {leg}"
                    ),
                }
            })?;
            by_transition_id
                .entry(transition_id)
                .or_default()
                .push(leg.clone());
        }
        Ok(Self { by_transition_id })
    }

    fn take_one(
        &mut self,
        transition_id: &str,
        kind: MarketOrderTransitionKind,
    ) -> OrderExecutionResult<Value> {
        let mut legs = self.take_all(transition_id);
        if legs.len() != 1 {
            return Err(OrderExecutionError::ProviderRequestFailed {
                provider: "refund".to_string(),
                message: format!(
                    "quoted refund transition {transition_id} ({}) expected exactly 1 leg, got {}",
                    kind.as_str(),
                    legs.len()
                ),
            });
        }
        Ok(legs.remove(0))
    }

    fn take_all(&mut self, transition_id: &str) -> Vec<Value> {
        self.by_transition_id
            .remove(transition_id)
            .unwrap_or_default()
    }
}

fn materialize_refund_transition_steps(
    order: &RouterOrder,
    position: &RefundSourcePosition,
    quoted_path: &RefundQuotedPath,
    refund_attempt_id: Uuid,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionResult<Vec<OrderExecutionStep>> {
    let mut legs = RefundQuoteLegIndex::from_legs(&quoted_path.legs)?;
    let mut steps = Vec::new();
    let mut step_index = 1;

    for (transition_index, transition) in quoted_path.path.transitions.iter().enumerate() {
        let is_final = transition_index + 1 == quoted_path.path.transitions.len();
        match transition.kind {
            MarketOrderTransitionKind::AcrossBridge => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let source = refund_external_source_binding(position, transition_index)?;
                steps.push(refund_transition_across_bridge_step(
                    order,
                    transition,
                    &leg,
                    source,
                    is_final,
                    refund_attempt_id,
                    step_index,
                    planned_at,
                )?);
                step_index += 1;
            }
            MarketOrderTransitionKind::UnitDeposit => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let source = refund_external_source_binding(position, transition_index)?;
                steps.push(refund_transition_unit_deposit_step(
                    order, transition, &leg, source, step_index, planned_at,
                )?);
                step_index += 1;
            }
            MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let source = refund_external_source_binding(position, transition_index)?;
                steps.push(refund_transition_hyperliquid_bridge_deposit_step(
                    order, transition, &leg, source, step_index, planned_at,
                )?);
                step_index += 1;
            }
            MarketOrderTransitionKind::HyperliquidTrade => {
                let transition_legs = legs.take_all(&transition.id);
                if transition_legs.is_empty() {
                    return Err(OrderExecutionError::ProviderRequestFailed {
                        provider: "refund".to_string(),
                        message: format!(
                            "quoted refund path is missing hyperliquid trade legs for transition {}",
                            transition.id
                        ),
                    });
                }
                let custody = refund_hyperliquid_binding(
                    position,
                    &quoted_path.path.transitions,
                    transition_index,
                )?;
                let prefund_first_trade = refund_trade_prefund_from_withdrawable(
                    &quoted_path.path.transitions,
                    transition_index,
                );
                for (leg_index, leg) in transition_legs.iter().enumerate() {
                    steps.push(refund_transition_hyperliquid_trade_step(
                        order,
                        transition,
                        leg,
                        &custody,
                        prefund_first_trade && leg_index == 0,
                        refund_attempt_id,
                        leg_index,
                        transition_legs.len(),
                        step_index,
                        planned_at,
                    )?);
                    step_index += 1;
                }
            }
            MarketOrderTransitionKind::UniversalRouterSwap => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let source = refund_external_source_binding(position, transition_index)?;
                steps.push(refund_transition_universal_router_swap_step(
                    order,
                    transition,
                    &leg,
                    source,
                    is_final,
                    refund_attempt_id,
                    step_index,
                    planned_at,
                )?);
                step_index += 1;
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let custody = refund_hyperliquid_binding(
                    position,
                    &quoted_path.path.transitions,
                    transition_index,
                )?;
                steps.push(refund_transition_unit_withdrawal_step(
                    order, transition, &leg, &custody, is_final, step_index, planned_at,
                )?);
                step_index += 1;
            }
        }
    }

    Ok(steps)
}

fn refund_position_start_node(position: &RefundSourcePosition) -> MarketOrderNode {
    match &position.handle {
        RefundSourceHandle::FundingVault(vault) => {
            MarketOrderNode::External(vault.deposit_asset.clone())
        }
        RefundSourceHandle::ExternalCustody(_) => MarketOrderNode::External(position.asset.clone()),
        RefundSourceHandle::HyperliquidSpot { canonical, .. } => MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical: *canonical,
        },
    }
}

fn refund_path_compatible_with_position(
    position: &RefundSourcePosition,
    path: &TransitionPath,
) -> bool {
    let Some(first) = path.transitions.first() else {
        return false;
    };
    match &position.handle {
        RefundSourceHandle::FundingVault(_) => false,
        RefundSourceHandle::ExternalCustody(vault) => {
            if first.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                && vault.role != CustodyVaultRole::DestinationExecution
            {
                return false;
            }
            true
        }
        RefundSourceHandle::HyperliquidSpot { .. } => matches!(
            first.kind,
            MarketOrderTransitionKind::HyperliquidTrade | MarketOrderTransitionKind::UnitWithdrawal
        ),
    }
}

fn refund_provider_allowed_for_new_routes(
    snapshot: Option<&crate::services::ProviderPolicySnapshot>,
    provider: &str,
) -> bool {
    let Some(snapshot) = snapshot else {
        return true;
    };
    let policy = snapshot.policy(provider);
    if policy.allows_new_routes() {
        return true;
    }
    let reason = if policy.reason.is_empty() {
        "provider_policy".to_string()
    } else {
        policy.reason.clone()
    };
    telemetry::record_provider_quote_blocked(provider, &reason);
    false
}

fn refund_path_providers_allowed(
    snapshot: Option<&crate::services::ProviderPolicySnapshot>,
    path: &TransitionPath,
) -> bool {
    path.transitions.iter().all(|transition| {
        refund_provider_allowed_for_new_routes(snapshot, transition.provider.as_str())
    })
}

fn refund_unit_path_compatible(unit: &dyn UnitProvider, path: &TransitionPath) -> bool {
    path.transitions
        .iter()
        .all(|transition| match transition.kind {
            MarketOrderTransitionKind::UnitDeposit => {
                unit.supports_deposit(&transition.input.asset)
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                unit.supports_withdrawal(&transition.output.asset)
            }
            _ => true,
        })
}

fn refund_exchange_path_compatible(exchange_id: &str, path: &TransitionPath) -> bool {
    path.transitions
        .iter()
        .all(|transition| match transition.kind {
            MarketOrderTransitionKind::HyperliquidTrade
            | MarketOrderTransitionKind::UniversalRouterSwap => {
                transition.provider.as_str() == exchange_id
            }
            _ => true,
        })
}

fn refund_position_quote_address(position: &RefundSourcePosition) -> OrderExecutionResult<String> {
    Ok(match &position.handle {
        RefundSourceHandle::FundingVault(vault) => vault.deposit_vault_address.clone(),
        RefundSourceHandle::ExternalCustody(vault) => vault.address.clone(),
        RefundSourceHandle::HyperliquidSpot { vault, .. } => vault.address.clone(),
    })
}

fn choose_better_refund_quote(
    candidate: RefundQuotedPath,
    current: Option<RefundQuotedPath>,
) -> OrderExecutionResult<Option<RefundQuotedPath>> {
    let candidate_out = parse_refund_amount("refund.amount_out", &candidate.amount_out)?;
    let Some(current) = current else {
        return Ok(Some(candidate));
    };
    let current_out = parse_refund_amount("refund.amount_out", &current.amount_out)?;
    if candidate_out > current_out {
        Ok(Some(candidate))
    } else {
        Ok(Some(current))
    }
}

fn parse_refund_amount(field: &'static str, value: &str) -> OrderExecutionResult<U256> {
    U256::from_str_radix(value, 10).map_err(|err| OrderExecutionError::ProviderRequestFailed {
        provider: "refund".to_string(),
        message: format!("invalid amount for {field}: {err}"),
    })
}

async fn quote_refund_bridge(
    bridge: &dyn BridgeProvider,
    request: BridgeQuoteRequest,
) -> OrderExecutionResult<Option<crate::services::action_providers::BridgeQuote>> {
    tokio::time::timeout(REFUND_PROVIDER_TIMEOUT, bridge.quote_bridge(request))
        .await
        .map_err(|_| OrderExecutionError::ProviderRequestFailed {
            provider: bridge.id().to_string(),
            message: "refund bridge quote timed out".to_string(),
        })?
        .map_err(|message| OrderExecutionError::ProviderRequestFailed {
            provider: bridge.id().to_string(),
            message,
        })
}

async fn quote_refund_exchange(
    exchange: &dyn ExchangeProvider,
    request: ExchangeQuoteRequest,
) -> OrderExecutionResult<Option<crate::services::action_providers::ExchangeQuote>> {
    tokio::time::timeout(REFUND_PROVIDER_TIMEOUT, exchange.quote_trade(request))
        .await
        .map_err(|_| OrderExecutionError::ProviderRequestFailed {
            provider: exchange.id().to_string(),
            message: "refund exchange quote timed out".to_string(),
        })?
        .map_err(|message| OrderExecutionError::ProviderRequestFailed {
            provider: exchange.id().to_string(),
            message,
        })
}

fn reserve_refund_hyperliquid_spot_send_quote_gas(
    field: &'static str,
    value: &str,
) -> OrderExecutionResult<String> {
    let amount = parse_refund_amount(field, value)?;
    let reserve = U256::from(REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW);
    if amount <= reserve {
        return Err(OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!(
                "amount must exceed Hyperliquid spot token transfer gas reserve {reserve}"
            ),
        });
    }
    Ok(amount.saturating_sub(reserve).to_string())
}

fn refund_transition_leg_json(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: &str,
    input_asset: &DepositAsset,
    output_asset: &DepositAsset,
    amount_in: &str,
    amount_out: &str,
    expires_at: chrono::DateTime<Utc>,
    raw: Value,
) -> Value {
    json!({
        "transition_decl_id": transition_decl_id,
        "transition_parent_decl_id": transition_decl_id,
        "transition_kind": transition_kind.as_str(),
        "provider": provider,
        "input_asset": {
            "chain_id": input_asset.chain.as_str(),
            "asset": input_asset.asset.as_str(),
        },
        "output_asset": {
            "chain_id": output_asset.chain.as_str(),
            "asset": output_asset.asset.as_str(),
        },
        "amount_in": amount_in,
        "amount_out": amount_out,
        "expires_at": expires_at.to_rfc3339(),
        "raw": raw,
    })
}

fn refund_exchange_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: &str,
    quote: &crate::services::action_providers::ExchangeQuote,
) -> OrderExecutionResult<Vec<Value>> {
    let kind = quote
        .provider_quote
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("");
    match kind {
        "spot_no_op" => Ok(vec![]),
        "universal_router_swap" => {
            let input_asset = quote
                .provider_quote
                .get("input_asset")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let output_asset = quote
                .provider_quote
                .get("output_asset")
                .cloned()
                .unwrap_or_else(|| json!({}));
            Ok(vec![json!({
                "transition_decl_id": transition_decl_id,
                "transition_parent_decl_id": transition_decl_id,
                "transition_kind": transition_kind.as_str(),
                "provider": provider,
                "input_asset": input_asset,
                "output_asset": output_asset,
                "amount_in": quote.amount_in,
                "amount_out": quote.amount_out,
                "expires_at": quote.expires_at.to_rfc3339(),
                "raw": quote.provider_quote.clone(),
            })])
        }
        "spot_cross_token" => {
            let Some(legs) = quote.provider_quote.get("legs").and_then(Value::as_array) else {
                return Err(OrderExecutionError::ProviderRequestFailed {
                    provider: provider.to_string(),
                    message: "refund exchange quote missing spot_cross_token legs".to_string(),
                });
            };
            Ok(legs
                .iter()
                .enumerate()
                .map(|(index, leg)| {
                    let input_asset =
                        leg.get("input_asset").cloned().unwrap_or_else(|| json!({}));
                    let output_asset =
                        leg.get("output_asset").cloned().unwrap_or_else(|| json!({}));
                    json!({
                        "transition_decl_id": format!("{transition_decl_id}:leg:{index}"),
                        "transition_parent_decl_id": transition_decl_id,
                        "transition_kind": transition_kind.as_str(),
                        "provider": provider,
                        "input_asset": input_asset,
                        "output_asset": output_asset,
                        "amount_in": leg.get("amount_in").cloned().unwrap_or_else(|| json!(quote.amount_in)),
                        "amount_out": leg.get("amount_out").cloned().unwrap_or_else(|| json!(quote.amount_out)),
                        "expires_at": quote.expires_at.to_rfc3339(),
                        "raw": leg.clone(),
                    })
                })
                .collect())
        }
        other => Err(OrderExecutionError::ProviderRequestFailed {
            provider: provider.to_string(),
            message: format!(
                "unsupported refund exchange quote kind in transition path: {other:?}"
            ),
        }),
    }
}

fn flatten_refund_transition_legs(legs_per_transition: Vec<Vec<Value>>) -> Vec<Value> {
    let mut flattened = Vec::new();
    for mut transition_legs in legs_per_transition {
        flattened.append(&mut transition_legs);
    }
    flattened
}

fn refund_external_source_binding(
    position: &RefundSourcePosition,
    transition_index: usize,
) -> OrderExecutionResult<RefundExternalSourceBinding> {
    if transition_index == 0 {
        let RefundSourceHandle::ExternalCustody(vault) = &position.handle else {
            return Err(OrderExecutionError::ProviderRequestFailed {
                provider: "refund".to_string(),
                message: "refund route expected an external custody source".to_string(),
            });
        };
        return Ok(RefundExternalSourceBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: vault.role,
        });
    }

    Ok(RefundExternalSourceBinding::DerivedDestinationExecution)
}

fn refund_hyperliquid_binding(
    position: &RefundSourcePosition,
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> OrderExecutionResult<RefundHyperliquidBinding> {
    if let RefundSourceHandle::HyperliquidSpot { vault, .. } = &position.handle {
        return Ok(RefundHyperliquidBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: CustodyVaultRole::HyperliquidSpot,
            asset: None,
        });
    }

    let prior_transitions = &transitions[..transition_index];
    if let RefundSourceHandle::ExternalCustody(vault) = &position.handle {
        if transitions.first().map(|transition| transition.kind)
            == Some(MarketOrderTransitionKind::HyperliquidBridgeDeposit)
            && prior_transitions
                .iter()
                .all(|transition| transition.kind != MarketOrderTransitionKind::AcrossBridge)
        {
            return Ok(RefundHyperliquidBinding::Explicit {
                vault_id: vault.id,
                address: vault.address.clone(),
                role: vault.role,
                asset: Some(position.asset.clone()),
            });
        }
    }

    if prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::UnitDeposit)
    {
        return Ok(RefundHyperliquidBinding::DerivedSpot);
    }

    let Some(bridge_transition) = prior_transitions
        .iter()
        .find(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
    else {
        return Err(OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!(
                "refund transition {} has no preceding Hyperliquid custody source",
                transitions[transition_index].id
            ),
        });
    };

    Ok(RefundHyperliquidBinding::DerivedDestinationExecution {
        asset: bridge_transition.input.asset.clone(),
    })
}

fn refund_trade_prefund_from_withdrawable(
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> bool {
    let prior_transitions = &transitions[..transition_index];
    prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
        && prior_transitions
            .iter()
            .filter(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidTrade)
            .count()
            == 0
}

fn refund_leg_parent_transition_id(leg: &Value) -> Option<String> {
    leg.get("transition_parent_decl_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            leg.get("transition_decl_id")
                .and_then(Value::as_str)
                .map(|value| {
                    value
                        .split_once(":leg:")
                        .map_or(value, |(parent, _)| parent)
                        .to_string()
                })
        })
}

fn refund_leg_provider<'a>(
    leg: &'a Value,
    transition: &TransitionDecl,
) -> OrderExecutionResult<&'a str> {
    leg.get("provider")
        .and_then(Value::as_str)
        .or_else(|| Some(transition.provider.as_str()))
        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!(
                "quoted refund leg for transition {} is missing provider",
                transition.id
            ),
        })
}

fn refund_required_str<'a>(value: &'a Value, key: &'static str) -> OrderExecutionResult<&'a str> {
    value.get(key).and_then(Value::as_str).ok_or_else(|| {
        OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!("refund quote leg missing string field {key}"),
        }
    })
}

fn refund_leg_asset(leg: &Value, field: &'static str) -> OrderExecutionResult<DepositAsset> {
    let obj = leg
        .get(field)
        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!("refund quote leg missing {field}"),
        })?;
    let chain_id = refund_required_str(obj, "chain_id")?;
    let asset_str = refund_required_str(obj, "asset")?;
    let chain =
        ChainId::parse(chain_id).map_err(|err| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!("refund quote leg {field} chain_id {chain_id:?}: {err}"),
        })?;
    let asset =
        AssetId::parse(asset_str).map_err(|err| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!("refund quote leg {field} asset {asset_str:?}: {err}"),
        })?;
    Ok(DepositAsset { chain, asset })
}

fn refund_required_u8(value: &Value, key: &'static str) -> OrderExecutionResult<u8> {
    let raw = value.get(key).and_then(Value::as_u64).ok_or_else(|| {
        OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: format!("refund quote leg missing numeric field {key}"),
        }
    })?;
    u8::try_from(raw).map_err(|err| OrderExecutionError::ProviderRequestFailed {
        provider: "refund".to_string(),
        message: format!("refund quote leg field {key} does not fit u8: {err}"),
    })
}

fn refund_planned_step(
    order_id: Uuid,
    transition_decl_id: Option<String>,
    step_index: i32,
    step_type: OrderExecutionStepType,
    provider: String,
    input_asset: Option<DepositAsset>,
    output_asset: Option<DepositAsset>,
    amount_in: Option<String>,
    min_amount_out: Option<String>,
    provider_ref: Option<String>,
    request: Value,
    details: Value,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionStep {
    OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id,
        execution_attempt_id: None,
        transition_decl_id,
        step_index,
        step_type,
        provider,
        status: OrderExecutionStepStatus::Planned,
        input_asset,
        output_asset,
        amount_in,
        min_amount_out,
        tx_hash: None,
        provider_ref,
        idempotency_key: None,
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        request,
        details,
        response: json!({}),
        error: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    }
}

fn refund_transition_across_bridge_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &Value,
    source: RefundExternalSourceBinding,
    is_final: bool,
    refund_attempt_id: Uuid,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionResult<OrderExecutionStep> {
    let provider = refund_leg_provider(leg, transition)?.to_string();
    let amount_in = refund_required_str(leg, "amount_in")?.to_string();
    let source_for_details = source.clone();
    let (depositor_id, depositor_address, depositor_role, refund_id, refund_address, refund_role) =
        match source {
            RefundExternalSourceBinding::Explicit {
                vault_id,
                address,
                role,
            } => (
                json!(vault_id),
                json!(address.clone()),
                json!(role.to_db_string()),
                json!(vault_id),
                json!(address),
                json!(role.to_db_string()),
            ),
            RefundExternalSourceBinding::DerivedDestinationExecution => (
                Value::Null,
                Value::Null,
                json!(CustodyVaultRole::DestinationExecution.to_db_string()),
                Value::Null,
                Value::Null,
                json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            ),
        };
    let recipient = if is_final {
        json!(order.refund_address)
    } else {
        Value::Null
    };
    let recipient_role = if is_final {
        Value::Null
    } else {
        json!(CustodyVaultRole::DestinationExecution.to_db_string())
    };
    Ok(refund_planned_step(
        order.id,
        Some(transition.id.clone()),
        step_index,
        OrderExecutionStepType::AcrossBridge,
        provider,
        Some(transition.input.asset.clone()),
        Some(transition.output.asset.clone()),
        Some(amount_in.clone()),
        None,
        Some(format!("refund-attempt-{refund_attempt_id}")),
        json!({
            "order_id": order.id,
            "origin_chain_id": transition.input.asset.chain.as_str(),
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient": recipient,
            "recipient_custody_vault_role": recipient_role,
            "recipient_custody_vault_id": null,
            "refund_address": refund_address,
            "refund_custody_vault_id": refund_id,
            "refund_custody_vault_role": refund_role,
            "partial_fills_enabled": false,
            "depositor_address": depositor_address,
            "depositor_custody_vault_id": depositor_id,
            "depositor_custody_vault_role": depositor_role,
        }),
        json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "depositor_custody_vault_role": match source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
        }),
        planned_at,
    ))
}

fn refund_transition_unit_deposit_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &Value,
    source: RefundExternalSourceBinding,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionResult<OrderExecutionStep> {
    let provider = refund_leg_provider(leg, transition)?.to_string();
    let amount_in = refund_required_str(leg, "amount_in")?.to_string();
    let (source_id, source_address, source_role, revert_id, revert_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (
            json!(vault_id),
            json!(address),
            json!(role.to_db_string()),
            json!(vault_id),
            json!(role.to_db_string()),
        ),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };
    Ok(refund_planned_step(
        order.id,
        Some(transition.id.clone()),
        step_index,
        OrderExecutionStepType::UnitDeposit,
        provider,
        Some(transition.input.asset.clone()),
        None,
        Some(amount_in.clone()),
        None,
        None,
        json!({
            "order_id": order.id,
            "src_chain_id": transition.input.asset.chain.as_str(),
            "dst_chain_id": "hyperliquid",
            "asset_id": transition.input.asset.asset.as_str(),
            "amount": amount_in,
            "source_custody_vault_id": source_id,
            "source_custody_vault_role": source_role,
            "source_custody_vault_address": source_address,
            "revert_custody_vault_id": revert_id,
            "revert_custody_vault_role": revert_role,
            "hyperliquid_custody_vault_role": CustodyVaultRole::HyperliquidSpot.to_db_string(),
            "hyperliquid_custody_vault_id": null,
            "hyperliquid_custody_vault_address": null,
        }),
        json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "hyperliquid_custody_vault_role": CustodyVaultRole::HyperliquidSpot.to_db_string(),
            "hyperliquid_custody_vault_status": "pending_derivation",
        }),
        planned_at,
    ))
}

fn refund_transition_hyperliquid_bridge_deposit_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &Value,
    source: RefundExternalSourceBinding,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionResult<OrderExecutionStep> {
    let provider = refund_leg_provider(leg, transition)?.to_string();
    let amount_in = refund_required_str(leg, "amount_in")?.to_string();
    let source_for_details = source.clone();
    let (source_id, source_address, source_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (json!(vault_id), json!(address), json!(role.to_db_string())),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };
    Ok(refund_planned_step(
        order.id,
        Some(transition.id.clone()),
        step_index,
        OrderExecutionStepType::HyperliquidBridgeDeposit,
        provider,
        Some(transition.input.asset.clone()),
        Some(transition.output.asset.clone()),
        Some(amount_in.clone()),
        None,
        None,
        json!({
            "order_id": order.id,
            "source_chain_id": transition.input.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "amount": amount_in,
            "source_custody_vault_id": source_id,
            "source_custody_vault_role": source_role,
            "source_custody_vault_address": source_address,
        }),
        json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "source_custody_vault_role": match source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            }
        }),
        planned_at,
    ))
}

fn refund_transition_hyperliquid_trade_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &Value,
    custody: &RefundHyperliquidBinding,
    prefund_from_withdrawable: bool,
    refund_attempt_id: Uuid,
    leg_index: usize,
    leg_count: usize,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionResult<OrderExecutionStep> {
    let provider = refund_leg_provider(leg, transition)?.to_string();
    let trade_leg = leg.get("raw").unwrap_or(leg);
    let order_kind = refund_required_str(trade_leg, "order_kind")?;
    let amount_in = leg
        .get("amount_in")
        .and_then(Value::as_str)
        .or_else(|| trade_leg.get("amount_in").and_then(Value::as_str))
        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: "hyperliquid refund trade leg missing amount_in".to_string(),
        })?;
    let amount_out = leg
        .get("amount_out")
        .and_then(Value::as_str)
        .or_else(|| trade_leg.get("amount_out").and_then(Value::as_str))
        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: "hyperliquid refund trade leg missing amount_out".to_string(),
        })?;
    let min_amount_out = trade_leg
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let max_amount_in = trade_leg
        .get("max_amount_in")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let input_asset = refund_leg_asset(leg, "input_asset")
        .or_else(|_| refund_leg_asset(trade_leg, "input_asset"))?;
    let output_asset = refund_leg_asset(leg, "output_asset")
        .or_else(|_| refund_leg_asset(trade_leg, "output_asset"))?;
    let request_input_asset = input_asset.clone();
    let request_output_asset = output_asset.clone();

    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) = match custody {
        RefundHyperliquidBinding::Explicit {
            vault_id,
            address,
            role,
            asset,
        } => (
            json!(vault_id),
            json!(address),
            json!(role.to_db_string()),
            json!(asset.as_ref().map(|asset| asset.chain.as_str())),
            json!(asset.as_ref().map(|asset| asset.asset.as_str())),
        ),
        RefundHyperliquidBinding::DerivedSpot => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string()),
            Value::Null,
            Value::Null,
        ),
        RefundHyperliquidBinding::DerivedDestinationExecution { asset } => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            json!(asset.chain.as_str()),
            json!(asset.asset.as_str()),
        ),
    };

    Ok(refund_planned_step(
        order.id,
        Some(transition.id.clone()),
        step_index,
        OrderExecutionStepType::HyperliquidTrade,
        provider,
        Some(input_asset),
        Some(output_asset),
        Some(amount_in.to_string()),
        min_amount_out.clone(),
        Some(format!("refund-attempt-{refund_attempt_id}")),
        json!({
            "order_id": order.id,
            "quote_id": refund_attempt_id,
            "leg_index": leg_index,
            "leg_count": leg_count,
            "order_kind": order_kind,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "min_amount_out": min_amount_out,
            "max_amount_in": max_amount_in,
            "input_asset": {
                "chain_id": request_input_asset.chain.as_str(),
                "asset": request_input_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": request_output_asset.chain.as_str(),
                "asset": request_output_asset.asset.as_str(),
            },
            "prefund_from_withdrawable": prefund_from_withdrawable,
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "leg_index": leg_index,
            "leg_count": leg_count,
            "hyperliquid_custody_vault_role": match custody {
                RefundHyperliquidBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundHyperliquidBinding::DerivedSpot => json!(CustodyVaultRole::HyperliquidSpot.to_db_string()),
                RefundHyperliquidBinding::DerivedDestinationExecution { .. } => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "hyperliquid_custody_vault_status": match custody {
                RefundHyperliquidBinding::Explicit { .. } => json!("bound"),
                _ => json!("pending_derivation"),
            },
        }),
        planned_at,
    ))
}

#[allow(clippy::too_many_arguments)]
fn refund_transition_universal_router_swap_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &Value,
    source: RefundExternalSourceBinding,
    is_final: bool,
    refund_attempt_id: Uuid,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionResult<OrderExecutionStep> {
    let provider = refund_leg_provider(leg, transition)?.to_string();
    let swap_leg = leg.get("raw").unwrap_or(leg);
    let order_kind = refund_required_str(swap_leg, "order_kind")?;
    let amount_in = leg
        .get("amount_in")
        .and_then(Value::as_str)
        .or_else(|| swap_leg.get("amount_in").and_then(Value::as_str))
        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: "universal router refund swap leg missing amount_in".to_string(),
        })?;
    let amount_out = leg
        .get("amount_out")
        .and_then(Value::as_str)
        .or_else(|| swap_leg.get("amount_out").and_then(Value::as_str))
        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: "universal router refund swap leg missing amount_out".to_string(),
        })?;
    let input_asset = refund_leg_asset(leg, "input_asset")
        .or_else(|_| refund_leg_asset(swap_leg, "input_asset"))?;
    let output_asset = refund_leg_asset(leg, "output_asset")
        .or_else(|_| refund_leg_asset(swap_leg, "output_asset"))?;
    let input_decimals = refund_required_u8(swap_leg, "src_decimals")?;
    let output_decimals = refund_required_u8(swap_leg, "dest_decimals")?;
    let price_route = swap_leg.get("price_route").cloned().ok_or_else(|| {
        OrderExecutionError::ProviderRequestFailed {
            provider: "refund".to_string(),
            message: "universal router refund swap leg missing price_route".to_string(),
        }
    })?;
    let min_amount_out = swap_leg
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let max_amount_in = swap_leg
        .get("max_amount_in")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let slippage_bps = swap_leg.get("slippage_bps").and_then(Value::as_u64);
    let source_for_details = source.clone();
    let (source_id, source_address, source_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (json!(vault_id), json!(address), json!(role.to_db_string())),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };

    Ok(refund_planned_step(
        order.id,
        Some(transition.id.clone()),
        step_index,
        OrderExecutionStepType::UniversalRouterSwap,
        provider,
        Some(input_asset.clone()),
        Some(output_asset.clone()),
        Some(amount_in.to_string()),
        min_amount_out.clone(),
        Some(format!("refund-attempt-{refund_attempt_id}")),
        json!({
            "order_id": order.id,
            "quote_id": refund_attempt_id,
            "order_kind": order_kind,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "min_amount_out": min_amount_out,
            "max_amount_in": max_amount_in,
            "input_asset": {
                "chain_id": input_asset.chain.as_str(),
                "asset": input_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": output_asset.chain.as_str(),
                "asset": output_asset.asset.as_str(),
            },
            "input_decimals": input_decimals,
            "output_decimals": output_decimals,
            "price_route": price_route,
            "slippage_bps": slippage_bps,
            "source_custody_vault_id": source_id,
            "source_custody_vault_address": source_address,
            "source_custody_vault_role": source_role,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
        }),
        json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "source_custody_vault_role": match source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
        }),
        planned_at,
    ))
}

fn refund_transition_unit_withdrawal_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &Value,
    custody: &RefundHyperliquidBinding,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionResult<OrderExecutionStep> {
    let provider = refund_leg_provider(leg, transition)?.to_string();
    let amount_in = refund_required_str(leg, "amount_in")?.to_string();
    let amount_out = refund_required_str(leg, "amount_out")?.to_string();
    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) = match custody {
        RefundHyperliquidBinding::Explicit {
            vault_id,
            address,
            role,
            asset,
        } => (
            json!(vault_id),
            json!(address),
            json!(role.to_db_string()),
            json!(asset.as_ref().map(|asset| asset.chain.as_str())),
            json!(asset.as_ref().map(|asset| asset.asset.as_str())),
        ),
        RefundHyperliquidBinding::DerivedSpot => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string()),
            Value::Null,
            Value::Null,
        ),
        RefundHyperliquidBinding::DerivedDestinationExecution { asset } => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            json!(asset.chain.as_str()),
            json!(asset.asset.as_str()),
        ),
    };

    Ok(refund_planned_step(
        order.id,
        Some(transition.id.clone()),
        step_index,
        OrderExecutionStepType::UnitWithdrawal,
        provider,
        Some(transition.input.asset.clone()),
        Some(transition.output.asset.clone()),
        Some(amount_in),
        Some("0".to_string()),
        None,
        json!({
            "order_id": order.id,
            "dst_chain_id": transition.output.asset.chain.as_str(),
            "asset_id": transition.output.asset.asset.as_str(),
            "amount": amount_out,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_id": null,
            "min_amount_out": "0",
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "hyperliquid_custody_vault_role": match custody {
                RefundHyperliquidBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundHyperliquidBinding::DerivedSpot => json!(CustodyVaultRole::HyperliquidSpot.to_db_string()),
                RefundHyperliquidBinding::DerivedDestinationExecution { .. } => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "hyperliquid_custody_vault_status": match custody {
                RefundHyperliquidBinding::Explicit { .. } => json!("bound"),
                _ => json!("pending_derivation"),
            },
        }),
        planned_at,
    ))
}

fn refund_positions_snapshot(positions: &[RefundSourcePosition]) -> Value {
    json!({
        "positions": positions.iter().map(|position| {
            let source_kind = match &position.handle {
                RefundSourceHandle::FundingVault(vault) => json!({
                    "kind": "funding_vault",
                    "vault_id": vault.id,
                    "address": vault.deposit_vault_address,
                }),
                RefundSourceHandle::ExternalCustody(vault) => json!({
                    "kind": "external_custody",
                    "vault_id": vault.id,
                    "address": vault.address,
                    "role": vault.role.to_db_string(),
                }),
                RefundSourceHandle::HyperliquidSpot { vault, coin, canonical } => json!({
                    "kind": "hyperliquid_spot",
                    "vault_id": vault.id,
                    "address": vault.address,
                    "coin": coin,
                    "canonical": canonical.as_str(),
                }),
            };
            json!({
                "source": source_kind,
                "asset": {
                    "chain": position.asset.chain.as_str(),
                    "asset": position.asset.asset.as_str(),
                },
                "amount": &position.amount,
            })
        }).collect::<Vec<_>>()
    })
}

fn asset_decimals(asset: &DepositAsset) -> u8 {
    match asset.chain.as_str() {
        "bitcoin" => 8,
        "evm:1" | "evm:8453" => match asset.asset {
            AssetId::Native => 18,
            _ => 6,
        },
        "evm:42161" => 6,
        _ => 8,
    }
}

fn decimal_to_raw_string(value: &str, decimals: u8) -> String {
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or("0");
    let fractional = parts.next().unwrap_or("");
    let mut normalized = whole
        .trim_start_matches('+')
        .trim_start_matches('0')
        .to_string();
    if normalized.is_empty() {
        normalized = "0".to_string();
    }
    let mut fractional = fractional.to_string();
    fractional.retain(|ch| ch.is_ascii_digit());
    if fractional.len() > decimals as usize {
        fractional.truncate(decimals as usize);
    } else {
        while fractional.len() < decimals as usize {
            fractional.push('0');
        }
    }
    let combined = format!("{normalized}{fractional}");
    let trimmed = combined.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

fn decimal_string_positive(value: &str) -> bool {
    value.chars().any(|ch| ch.is_ascii_digit() && ch != '0')
}

fn validate_materialized_intermediate_custody_step(
    order_id: Uuid,
    step: &OrderExecutionStep,
    is_final: bool,
    vaults_by_id: &HashMap<Uuid, &CustodyVault>,
) -> OrderExecutionResult<()> {
    match step.step_type {
        OrderExecutionStepType::AcrossBridge => {
            maybe_validate_bound_internal_vault(
                order_id,
                step,
                "depositor_custody_vault_role",
                "depositor_custody_vault_id",
                Some("depositor_address"),
                &[CustodyVaultRole::DestinationExecution],
                vaults_by_id,
            )?;
            maybe_validate_bound_internal_vault(
                order_id,
                step,
                "refund_custody_vault_role",
                "refund_custody_vault_id",
                Some("refund_address"),
                &[CustodyVaultRole::DestinationExecution],
                vaults_by_id,
            )?;
            if !is_final {
                validate_bound_internal_vault(
                    order_id,
                    step,
                    "recipient_custody_vault_role",
                    "recipient_custody_vault_id",
                    Some("recipient"),
                    &[CustodyVaultRole::DestinationExecution],
                    vaults_by_id,
                )?;
            }
        }
        OrderExecutionStepType::UnitDeposit => {
            maybe_validate_bound_internal_vault(
                order_id,
                step,
                "source_custody_vault_role",
                "source_custody_vault_id",
                Some("source_custody_vault_address"),
                &[CustodyVaultRole::DestinationExecution],
                vaults_by_id,
            )?;
            maybe_validate_bound_internal_vault(
                order_id,
                step,
                "revert_custody_vault_role",
                "revert_custody_vault_id",
                None,
                &[CustodyVaultRole::DestinationExecution],
                vaults_by_id,
            )?;
            validate_bound_internal_vault(
                order_id,
                step,
                "hyperliquid_custody_vault_role",
                "hyperliquid_custody_vault_id",
                Some("hyperliquid_custody_vault_address"),
                &[CustodyVaultRole::HyperliquidSpot],
                vaults_by_id,
            )?;
        }
        OrderExecutionStepType::HyperliquidBridgeDeposit => {
            validate_bound_internal_vault(
                order_id,
                step,
                "source_custody_vault_role",
                "source_custody_vault_id",
                Some("source_custody_vault_address"),
                &[CustodyVaultRole::DestinationExecution],
                vaults_by_id,
            )?;
        }
        OrderExecutionStepType::HyperliquidTrade => {
            validate_bound_internal_vault(
                order_id,
                step,
                "hyperliquid_custody_vault_role",
                "hyperliquid_custody_vault_id",
                Some("hyperliquid_custody_vault_address"),
                &[
                    CustodyVaultRole::DestinationExecution,
                    CustodyVaultRole::HyperliquidSpot,
                ],
                vaults_by_id,
            )?;
        }
        OrderExecutionStepType::UniversalRouterSwap => {
            maybe_validate_bound_internal_vault(
                order_id,
                step,
                "source_custody_vault_role",
                "source_custody_vault_id",
                Some("source_custody_vault_address"),
                &[CustodyVaultRole::DestinationExecution],
                vaults_by_id,
            )?;
            if !is_final {
                validate_bound_internal_vault(
                    order_id,
                    step,
                    "recipient_custody_vault_role",
                    "recipient_custody_vault_id",
                    Some("recipient_address"),
                    &[CustodyVaultRole::DestinationExecution],
                    vaults_by_id,
                )?;
            }
        }
        OrderExecutionStepType::UnitWithdrawal => {
            if !is_final {
                validate_bound_internal_vault(
                    order_id,
                    step,
                    "recipient_custody_vault_role",
                    "recipient_custody_vault_id",
                    Some("recipient_address"),
                    &[CustodyVaultRole::DestinationExecution],
                    vaults_by_id,
                )?;
            }
            validate_bound_internal_vault(
                order_id,
                step,
                "hyperliquid_custody_vault_role",
                "hyperliquid_custody_vault_id",
                Some("hyperliquid_custody_vault_address"),
                &[
                    CustodyVaultRole::DestinationExecution,
                    CustodyVaultRole::HyperliquidSpot,
                ],
                vaults_by_id,
            )?;
        }
        OrderExecutionStepType::WaitForDeposit | OrderExecutionStepType::Refund => {
            return Err(OrderExecutionError::IntermediateCustodyInvariant {
                step_id: step.id,
                reason: "internal-only steps must not appear in market-order execution routes"
                    .to_string(),
            });
        }
    }

    Ok(())
}

fn maybe_validate_bound_internal_vault(
    order_id: Uuid,
    step: &OrderExecutionStep,
    role_field: &'static str,
    id_field: &'static str,
    address_field: Option<&'static str>,
    allowed_roles: &[CustodyVaultRole],
    vaults_by_id: &HashMap<Uuid, &CustodyVault>,
) -> OrderExecutionResult<()> {
    if step
        .request
        .get(role_field)
        .is_none_or(serde_json::Value::is_null)
    {
        return Ok(());
    }
    validate_bound_internal_vault(
        order_id,
        step,
        role_field,
        id_field,
        address_field,
        allowed_roles,
        vaults_by_id,
    )
}

fn validate_bound_internal_vault(
    order_id: Uuid,
    step: &OrderExecutionStep,
    role_field: &'static str,
    id_field: &'static str,
    address_field: Option<&'static str>,
    allowed_roles: &[CustodyVaultRole],
    vaults_by_id: &HashMap<Uuid, &CustodyVault>,
) -> OrderExecutionResult<()> {
    let role = request_custody_role(step, role_field)?;
    if !allowed_roles.contains(&role) {
        return Err(OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!(
                "{role_field} must be one of [{}], got {}",
                allowed_roles
                    .iter()
                    .map(|role| role.to_db_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                role.to_db_string(),
            ),
        });
    }

    let vault_id = request_uuid(step, id_field)?;
    let Some(vault) = vaults_by_id.get(&vault_id).copied() else {
        return Err(OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!("{id_field} references unknown custody vault {vault_id}"),
        });
    };
    if vault.order_id != Some(order_id) {
        return Err(OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!(
                "{id_field} references custody vault {} that does not belong to order {}",
                vault.id, order_id
            ),
        });
    }
    if vault.visibility != CustodyVaultVisibility::Internal {
        return Err(OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!(
                "{id_field} references custody vault {} with non-internal visibility {}",
                vault.id,
                vault.visibility.to_db_string(),
            ),
        });
    }
    if vault.control_type != CustodyVaultControlType::RouterDerivedKey {
        return Err(OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!(
                "{id_field} references custody vault {} with non-router-derived control_type {}",
                vault.id,
                vault.control_type.to_db_string(),
            ),
        });
    }
    if vault.role != role {
        return Err(OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!(
                "{id_field} references custody vault {} with role {}, expected {}",
                vault.id,
                vault.role.to_db_string(),
                role.to_db_string(),
            ),
        });
    }

    if let Some(address_field) = address_field {
        let address = request_str(step, address_field)?;
        if !address_matches(&vault.address, address) {
            return Err(OrderExecutionError::IntermediateCustodyInvariant {
                step_id: step.id,
                reason: format!(
                    "{address_field} {} does not match custody vault {} address {}",
                    address, vault.id, vault.address
                ),
            });
        }
    }

    Ok(())
}

fn request_uuid(step: &OrderExecutionStep, field: &'static str) -> OrderExecutionResult<Uuid> {
    let raw = request_str(step, field)?;
    Uuid::parse_str(raw).map_err(|err| OrderExecutionError::IntermediateCustodyInvariant {
        step_id: step.id,
        reason: format!("{field} must be a UUID string: {err}"),
    })
}

fn request_custody_role(
    step: &OrderExecutionStep,
    field: &'static str,
) -> OrderExecutionResult<CustodyVaultRole> {
    let raw = request_str(step, field)?;
    CustodyVaultRole::from_db_string(raw).ok_or_else(|| {
        OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!("{field} contains unknown custody role {raw:?}"),
        }
    })
}

fn request_str<'a>(
    step: &'a OrderExecutionStep,
    field: &'static str,
) -> OrderExecutionResult<&'a str> {
    step.request
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| OrderExecutionError::IntermediateCustodyInvariant {
            step_id: step.id,
            reason: format!("request is missing non-empty {field}"),
        })
}

fn json_object_or_wrapped(value: serde_json::Value) -> serde_json::Value {
    if value.is_object() {
        value
    } else {
        json!({ "value": value })
    }
}

fn json_string_equals(value: &Value, key: &str, expected: &str) -> bool {
    value.get(key).and_then(Value::as_str) == Some(expected)
}

fn uuid_from_json_field(value: &Value, key: &str) -> Option<Uuid> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

fn step_min_amount_out_raw(
    step: &OrderExecutionStep,
    provider: &str,
) -> OrderExecutionResult<Option<U256>> {
    let raw = step
        .min_amount_out
        .as_deref()
        .or_else(|| step.request.get("min_amount_out").and_then(Value::as_str));
    let Some(raw) = raw else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    U256::from_str_radix(raw, 10).map(Some).map_err(|err| {
        OrderExecutionError::ProviderRequestFailed {
            provider: provider.to_string(),
            message: format!("invalid min_amount_out {raw:?}: {err}"),
        }
    })
}

fn set_json_value(target: &mut Value, key: &str, value: Value) {
    if !target.is_object() {
        *target = json!({});
    }
    if let Some(object) = target.as_object_mut() {
        object.insert(key.to_string(), value);
    }
}

fn required_evidence_str<'a>(
    hint: &'a OrderProviderOperationHint,
    field: &str,
) -> OrderExecutionResult<&'a str> {
    hint.evidence
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| OrderExecutionError::InvalidProviderOperationHint {
            hint_id: hint.id,
            reason: format!("evidence is missing non-empty {field}"),
        })
}

fn evidence_amount(hint: &OrderProviderOperationHint, field: &str) -> OrderExecutionResult<U256> {
    let raw = required_evidence_str(hint, field)?;
    U256::from_str(raw).map_err(|err| OrderExecutionError::InvalidProviderOperationHint {
        hint_id: hint.id,
        reason: format!("evidence {field} is not a decimal base-unit amount: {err}"),
    })
}

fn evidence_u64(hint: &OrderProviderOperationHint, field: &str) -> OrderExecutionResult<u64> {
    let value = hint.evidence.get(field).ok_or_else(|| {
        OrderExecutionError::InvalidProviderOperationHint {
            hint_id: hint.id,
            reason: format!("evidence is missing {field}"),
        }
    })?;
    if let Some(value) = value.as_u64() {
        return Ok(value);
    }
    if let Some(raw) = value.as_str() {
        return raw.parse::<u64>().map_err(|err| {
            OrderExecutionError::InvalidProviderOperationHint {
                hint_id: hint.id,
                reason: format!("evidence {field} is not a u64: {err}"),
            }
        });
    }
    Err(OrderExecutionError::InvalidProviderOperationHint {
        hint_id: hint.id,
        reason: format!("evidence {field} must be a u64 or decimal string"),
    })
}

fn expected_operation_amount(
    hint_id: Uuid,
    operation: &OrderProviderOperation,
    step: Option<&OrderExecutionStep>,
) -> OrderExecutionResult<U256> {
    if let Some(expected) = operation
        .request
        .get("expected_amount")
        .and_then(Value::as_str)
    {
        return U256::from_str(expected).map_err(|err| {
            OrderExecutionError::InvalidProviderOperationHint {
                hint_id,
                reason: format!("operation expected_amount is invalid: {err}"),
            }
        });
    }
    if let Some(amount_in) = step.and_then(|step| step.amount_in.as_deref()) {
        return U256::from_str(amount_in).map_err(|err| {
            OrderExecutionError::InvalidProviderOperationHint {
                hint_id,
                reason: format!("step amount_in is invalid: {err}"),
            }
        });
    }

    Ok(U256::from(1_u64))
}

fn address_matches(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn internal_custody_terminal_status(
    order_status: RouterOrderStatus,
) -> Option<(CustodyVaultStatus, &'static str)> {
    match order_status {
        RouterOrderStatus::Completed => Some((CustodyVaultStatus::Released, "order_completed")),
        RouterOrderStatus::RefundRequired => {
            Some((CustodyVaultStatus::Failed, "order_refund_required"))
        }
        RouterOrderStatus::Refunding => Some((CustodyVaultStatus::Failed, "order_refunding")),
        RouterOrderStatus::Refunded => Some((CustodyVaultStatus::Failed, "order_refunded")),
        RouterOrderStatus::RefundManualInterventionRequired => Some((
            CustodyVaultStatus::Failed,
            "order_refund_manual_intervention_required",
        )),
        RouterOrderStatus::Failed => Some((CustodyVaultStatus::Failed, "order_failed")),
        RouterOrderStatus::Expired => Some((CustodyVaultStatus::Failed, "order_expired")),
        RouterOrderStatus::Quoted
        | RouterOrderStatus::PendingFunding
        | RouterOrderStatus::Funded
        | RouterOrderStatus::Executing => None,
    }
}

fn provider_status_observation_update(
    operation: &OrderProviderOperation,
    hint: &OrderProviderOperationHint,
    observation: ProviderOperationObservation,
) -> ProviderOperationStatusUpdate {
    let observed_state = json_object_or_wrapped(observation.observed_state);

    ProviderOperationStatusUpdate {
        provider_operation_id: Some(operation.id),
        provider: Some(operation.provider.clone()),
        provider_ref: observation
            .provider_ref
            .clone()
            .or_else(|| operation.provider_ref.clone()),
        status: observation.status,
        observed_state: json!({
            "source": "router_worker_provider_observation",
            "hint_id": hint.id,
            "hint_source": &hint.source,
            "hint_evidence": &hint.evidence,
            "provider_observed_state": observed_state,
        }),
        response: observation.response,
        tx_hash: observation.tx_hash,
        error: observation.error,
    }
}

fn persisted_provider_operation_tx_hash(operation: &OrderProviderOperation) -> Option<String> {
    operation
        .response
        .get("tx_hash")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            operation
                .response
                .get("tx_hashes")
                .and_then(Value::as_array)
                .and_then(|hashes| hashes.last())
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            operation
                .response
                .get("response")
                .and_then(|value| value.get("tx_hash"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
}

fn hyperliquid_timeout_due(
    operation: &OrderProviderOperation,
    observation: Option<&ProviderOperationObservation>,
) -> bool {
    if operation.operation_type != ProviderOperationType::HyperliquidTrade {
        return false;
    }
    let Some(observation) = observation else {
        return false;
    };
    if observation.status != ProviderOperationStatus::WaitingExternal {
        return false;
    }
    let Some(timeout_at_ms) = operation
        .request
        .get("timeout_at_ms")
        .and_then(Value::as_u64)
    else {
        return false;
    };
    (Utc::now().timestamp_millis().max(0) as u64) >= timeout_at_ms
}

fn execution_attempt_idempotency_key(
    order_id: Uuid,
    attempt_index: i32,
    provider: &str,
    step_index: i32,
) -> Option<String> {
    Some(format!(
        "order:{order_id}:attempt:{attempt_index}:{provider}:{step_index}"
    ))
}

fn failed_attempt_snapshot(order: &RouterOrder, failed_step: &OrderExecutionStep) -> Value {
    let source_kind = if failed_step.request.get("source_custody_vault_id").is_some() {
        "external_custody"
    } else if failed_step
        .request
        .get("hyperliquid_custody_vault_id")
        .is_some()
    {
        "hyperliquid_custody"
    } else {
        "funding_vault"
    };
    let source_asset = failed_step
        .input_asset
        .clone()
        .or_else(|| failed_step.output_asset.clone())
        .map(|asset| {
            json!({
                "chain": asset.chain.as_str(),
                "asset": asset.asset.as_str(),
            })
        })
        .unwrap_or_else(|| {
            json!({
                "chain": order.source_asset.chain.as_str(),
                "asset": order.source_asset.asset.as_str(),
            })
        });

    json!({
        "source_kind": source_kind,
        "funding_vault_id": order.funding_vault_id,
        "failed_step_id": failed_step.id,
        "failed_step_index": failed_step.step_index,
        "failed_step_type": failed_step.step_type.to_db_string(),
        "amount_in": failed_step.amount_in,
        "min_amount_out": failed_step.min_amount_out,
        "source_asset": source_asset,
        "source_custody_vault_id": failed_step.request.get("source_custody_vault_id").cloned(),
        "source_custody_vault_role": failed_step.request.get("source_custody_vault_role").cloned(),
        "source_custody_vault_address": failed_step
            .request
            .get("source_custody_vault_address")
            .cloned(),
        "hyperliquid_custody_vault_id": failed_step
            .request
            .get("hyperliquid_custody_vault_id")
            .cloned(),
        "hyperliquid_custody_vault_role": failed_step
            .request
            .get("hyperliquid_custody_vault_role")
            .cloned(),
        "hyperliquid_custody_vault_address": failed_step
            .request
            .get("hyperliquid_custody_vault_address")
            .cloned(),
        "recipient_custody_vault_id": failed_step
            .request
            .get("recipient_custody_vault_id")
            .cloned(),
        "recipient_custody_vault_role": failed_step
            .request
            .get("recipient_custody_vault_role")
            .cloned(),
        "recipient": failed_step.request.get("recipient").cloned(),
        "revert_custody_vault_id": failed_step.request.get("revert_custody_vault_id").cloned(),
        "revert_custody_vault_role": failed_step.request.get("revert_custody_vault_role").cloned(),
        "revert_custody_vault_address": failed_step
            .details
            .get("revert_custody_vault_address")
            .cloned(),
    })
}

fn token_identifier(asset: &AssetId) -> TokenIdentifier {
    match asset {
        AssetId::Native => TokenIdentifier::Native,
        AssetId::Reference(value) => TokenIdentifier::address(value.clone()),
    }
}

fn currency_decimals(chain: &router_primitives::ChainType, asset: &AssetId) -> u8 {
    match (chain, asset) {
        (router_primitives::ChainType::Bitcoin, AssetId::Native) => 8,
        (_, AssetId::Native) => 18,
        (_, AssetId::Reference(_)) => 8,
    }
}

fn is_unique_violation(err: &sqlx_core::Error) -> bool {
    match err {
        sqlx_core::Error::Database(db_err) => db_err.code().as_deref() == Some("23505"),
        _ => false,
    }
}

fn finalize_summary(
    progressed: bool,
    order_id: Uuid,
    completed_steps: usize,
) -> Option<OrderExecutionSummary> {
    if progressed {
        Some(OrderExecutionSummary {
            order_id,
            completed_steps,
        })
    } else {
        None
    }
}

fn apply_post_execute_patch(
    state: &mut ProviderExecutionState,
    patch: ProviderExecutionStatePatch,
    provider: &str,
) -> OrderExecutionResult<()> {
    let Some(operation) = state.operation.as_mut() else {
        return Err(OrderExecutionError::ProviderRequestFailed {
            provider: provider.to_string(),
            message: "custody_actions intent must include a provider operation in state"
                .to_string(),
        });
    };
    if let Some(provider_ref) = patch.provider_ref {
        operation.provider_ref = Some(provider_ref);
    }
    if let Some(observed_state) = patch.observed_state {
        operation.observed_state = Some(observed_state);
    }
    if let Some(response) = patch.response {
        operation.response = Some(response);
    }
    if let Some(status) = patch.status {
        operation.status = status;
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum PlannedRetentionAction {
    Transfer(CustodyAction),
    HyperliquidSpotSend {
        destination: String,
        token: String,
        raw_amount: U256,
        decimals: u8,
    },
}

impl PlannedRetentionAction {
    fn to_custody_action(
        &self,
        template: Option<&CustodyAction>,
        provider: &str,
    ) -> OrderExecutionResult<CustodyAction> {
        match self {
            Self::Transfer(action) => Ok(action.clone()),
            Self::HyperliquidSpotSend {
                destination,
                token,
                raw_amount,
                decimals,
            } => {
                let template = template
                    .and_then(hyperliquid_call_template)
                    .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
                        provider: provider.to_string(),
                        message: "hyperliquid retention requires a hyperliquid provider action"
                            .to_string(),
                    })?;
                Ok(CustodyAction::Call(ChainCall::Hyperliquid(
                    HyperliquidCall {
                        target_base_url: template.target_base_url.clone(),
                        network: template.network,
                        vault_address: template.vault_address.clone(),
                        payload: HyperliquidCallPayload::SpotSend {
                            destination: destination.clone(),
                            token: token.clone(),
                            amount: raw_to_decimal_string(*raw_amount, *decimals),
                        },
                    },
                )))
            }
        }
    }
}

struct PreparedCustodyAction {
    action: CustodyAction,
    provider_action: bool,
}

struct PreparedCustodyActions {
    actions: Vec<PreparedCustodyAction>,
    provider_action_count: usize,
}

fn prepare_custody_actions_with_retention(
    provider_actions: Vec<CustodyAction>,
    retention_actions: Vec<PlannedRetentionAction>,
    provider: &str,
) -> OrderExecutionResult<PreparedCustodyActions> {
    if retention_actions.is_empty() {
        return Ok(PreparedCustodyActions {
            provider_action_count: provider_actions.len(),
            actions: provider_actions
                .into_iter()
                .map(|action| PreparedCustodyAction {
                    action,
                    provider_action: true,
                })
                .collect(),
        });
    }

    let mut prepared = Vec::new();
    let mut hyperliquid_retentions = Vec::new();
    for retention in retention_actions {
        match retention {
            PlannedRetentionAction::Transfer(action) => prepared.push(PreparedCustodyAction {
                action,
                provider_action: false,
            }),
            action @ PlannedRetentionAction::HyperliquidSpotSend { .. } => {
                hyperliquid_retentions.push(action);
            }
        }
    }

    if hyperliquid_retentions.is_empty() {
        let provider_action_count = provider_actions.len();
        prepared.extend(
            provider_actions
                .into_iter()
                .map(|action| PreparedCustodyAction {
                    action,
                    provider_action: true,
                }),
        );
        return Ok(PreparedCustodyActions {
            actions: prepared,
            provider_action_count,
        });
    }

    let template = provider_actions
        .iter()
        .find_map(hyperliquid_call_template)
        .ok_or_else(|| OrderExecutionError::ProviderRequestFailed {
            provider: provider.to_string(),
            message: "hyperliquid retention requires hyperliquid provider actions".to_string(),
        })?
        .clone();
    let mut provider_actions = provider_actions.into_iter();
    let Some(mut first_provider_action) = provider_actions.next() else {
        return Err(OrderExecutionError::ProviderRequestFailed {
            provider: provider.to_string(),
            message: "hyperliquid retention requires at least one provider action".to_string(),
        });
    };
    let mut total_prefund = U256::ZERO;
    let mut prefund_decimals: Option<u8> = None;
    for retention in &hyperliquid_retentions {
        let PlannedRetentionAction::HyperliquidSpotSend {
            raw_amount,
            decimals,
            ..
        } = retention
        else {
            unreachable!("filtered above");
        };
        if prefund_decimals.is_some_and(|existing| existing != *decimals) {
            return Err(OrderExecutionError::ProviderRequestFailed {
                provider: provider.to_string(),
                message: "hyperliquid retention actions must use one decimal precision".to_string(),
            });
        }
        prefund_decimals = Some(*decimals);
        total_prefund = total_prefund.saturating_add(*raw_amount);
    }
    if let Some(decimals) = prefund_decimals {
        bump_hyperliquid_prefund_amount(
            &mut first_provider_action,
            total_prefund,
            decimals,
            provider,
        )?;
    }
    prepared.push(PreparedCustodyAction {
        action: first_provider_action,
        provider_action: true,
    });
    for retention in hyperliquid_retentions {
        prepared.push(PreparedCustodyAction {
            action: retention.to_custody_action(
                Some(&CustodyAction::Call(ChainCall::Hyperliquid(
                    template.clone(),
                ))),
                provider,
            )?,
            provider_action: false,
        });
    }
    let mut provider_action_count = 1;
    for action in provider_actions {
        provider_action_count += 1;
        prepared.push(PreparedCustodyAction {
            action,
            provider_action: true,
        });
    }

    Ok(PreparedCustodyActions {
        actions: prepared,
        provider_action_count,
    })
}

fn hyperliquid_call_template(action: &CustodyAction) -> Option<&HyperliquidCall> {
    match action {
        CustodyAction::Call(ChainCall::Hyperliquid(call)) => Some(call),
        _ => None,
    }
}

fn bump_hyperliquid_prefund_amount(
    action: &mut CustodyAction,
    extra_raw: U256,
    decimals: u8,
    provider: &str,
) -> OrderExecutionResult<()> {
    let CustodyAction::Call(ChainCall::Hyperliquid(HyperliquidCall {
        payload: HyperliquidCallPayload::UsdClassTransfer { amount, .. },
        ..
    })) = action
    else {
        return Ok(());
    };
    let current_raw = decimal_string_to_raw(amount, decimals).map_err(|message| {
        OrderExecutionError::ProviderRequestFailed {
            provider: provider.to_string(),
            message,
        }
    })?;
    *amount = raw_to_decimal_string(current_raw.saturating_add(extra_raw), decimals);
    Ok(())
}

fn raw_to_decimal_string(raw: U256, decimals: u8) -> String {
    let digits = raw.to_string();
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return digits;
    }
    let padded = if digits.len() <= decimals {
        format!("{:0>width$}", digits, width = decimals + 1)
    } else {
        digits
    };
    let split_at = padded.len() - decimals;
    let (whole, frac) = padded.split_at(split_at);
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{frac}")
    }
}

fn decimal_string_to_raw(value: &str, decimals: u8) -> Result<U256, String> {
    let trimmed = value.trim();
    let mut parts = trimmed.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || !frac.chars().all(|ch| ch.is_ascii_digit())
        || frac.len() > usize::from(decimals)
    {
        return Err(format!("invalid decimal amount {value:?}"));
    }
    let combined = format!("{whole}{:0<width$}", frac, width = usize::from(decimals));
    let digits = combined.trim_start_matches('0');
    U256::from_str_radix(if digits.is_empty() { "0" } else { digits }, 10)
        .map_err(|err| format!("invalid decimal amount {value:?}: {err}"))
}

fn provider_operation_outcome(
    provider: &str,
    operation: Option<&crate::services::action_providers::ProviderOperationIntent>,
) -> OrderExecutionResult<RunningStepOutcome> {
    match operation.map(|operation| operation.status) {
        None | Some(ProviderOperationStatus::Completed) => Ok(RunningStepOutcome::Completed),
        Some(
            ProviderOperationStatus::Planned
            | ProviderOperationStatus::Submitted
            | ProviderOperationStatus::WaitingExternal,
        ) => Ok(RunningStepOutcome::Waiting),
        Some(ProviderOperationStatus::Failed | ProviderOperationStatus::Expired) => {
            Err(OrderExecutionError::ProviderRequestFailed {
                provider: provider.to_string(),
                message: "provider returned a terminal failure operation state".to_string(),
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepExecutionOutcome {
    Completed,
    Waiting,
    /// The step was not progressed by this worker because another worker
    /// won the Ready/Planned → Running CAS. Callers should treat this as
    /// a clean no-op and back off from the order.
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunningStepOutcome {
    Completed,
    Waiting,
}

impl From<RunningStepOutcome> for StepExecutionOutcome {
    fn from(value: RunningStepOutcome) -> Self {
        match value {
            RunningStepOutcome::Completed => Self::Completed,
            RunningStepOutcome::Waiting => Self::Waiting,
        }
    }
}

#[derive(Debug, Clone)]
struct StepBalanceObservation {
    probes: Vec<StepBalanceProbe>,
}

impl StepBalanceObservation {
    fn for_step(step: &OrderExecutionStep, source_custody_vault_id: Uuid) -> Self {
        let mut probes = Vec::new();
        if let Some(asset) = step.input_asset.clone() {
            probes.push(StepBalanceProbe::new(
                BalanceProbeRole::Source,
                source_custody_vault_id,
                asset,
            ));
        }
        if let (Some(vault_id), Some(asset)) = (
            uuid_from_json_field(&step.request, "recipient_custody_vault_id"),
            step.output_asset.clone(),
        ) {
            probes.push(StepBalanceProbe::new(
                BalanceProbeRole::Destination,
                vault_id,
                asset,
            ));
        }
        Self { probes }
    }

    async fn capture_before(
        &mut self,
        manager: &OrderExecutionManager,
    ) -> OrderExecutionResult<()> {
        for probe in &mut self.probes {
            let (address, balance) = probe.read_balance(manager).await?;
            probe.address = Some(address);
            probe.before = balance;
        }
        Ok(())
    }

    async fn capture_after(&mut self, manager: &OrderExecutionManager) -> OrderExecutionResult<()> {
        for probe in &mut self.probes {
            let (address, balance) = probe.read_balance(manager).await?;
            probe.address.get_or_insert(address);
            probe.after = balance;
        }
        Ok(())
    }

    fn enforce_output_minimum(
        &self,
        step: &OrderExecutionStep,
        provider: &str,
    ) -> OrderExecutionResult<()> {
        let Some(min_amount_out) = step_min_amount_out_raw(step, provider)? else {
            return Ok(());
        };
        for probe in self
            .probes
            .iter()
            .filter(|probe| probe.role == BalanceProbeRole::Destination)
        {
            let Some(credit_delta) = probe.credit_delta() else {
                continue;
            };
            if credit_delta < min_amount_out {
                return Err(OrderExecutionError::ProviderRequestFailed {
                    provider: provider.to_string(),
                    message: format!(
                        "observed destination balance delta {} for {} on {} was below min_amount_out {}",
                        credit_delta,
                        probe.asset.asset.as_str(),
                        probe.asset.chain.as_str(),
                        min_amount_out,
                    ),
                });
            }
        }
        Ok(())
    }

    fn to_json(&self) -> Value {
        json!({
            "schema_version": 1,
            "probes": self.probes.iter().map(StepBalanceProbe::to_json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone)]
struct StepBalanceProbe {
    role: BalanceProbeRole,
    vault_id: Uuid,
    address: Option<String>,
    asset: DepositAsset,
    before: Option<U256>,
    after: Option<U256>,
}

impl StepBalanceProbe {
    fn new(role: BalanceProbeRole, vault_id: Uuid, asset: DepositAsset) -> Self {
        Self {
            role,
            vault_id,
            address: None,
            asset,
            before: None,
            after: None,
        }
    }

    async fn read_balance(
        &self,
        manager: &OrderExecutionManager,
    ) -> OrderExecutionResult<(String, Option<U256>)> {
        let vault = manager
            .db
            .orders()
            .get_custody_vault(self.vault_id)
            .await
            .map_err(|source| OrderExecutionError::Database { source })?;
        if !custody_vault_balance_is_chain_observable(&vault, &self.asset) {
            return Ok((vault.address, None));
        }
        let balance = manager
            .observed_balance_for_asset(&self.asset, &vault.address)
            .await?;
        Ok((vault.address, balance))
    }

    fn credit_delta(&self) -> Option<U256> {
        Some(self.after?.saturating_sub(self.before?))
    }

    fn debit_delta(&self) -> Option<U256> {
        Some(self.before?.saturating_sub(self.after?))
    }

    fn to_json(&self) -> Value {
        json!({
            "role": self.role.as_str(),
            "vault_id": self.vault_id,
            "address": self.address,
            "asset": {
                "chain": self.asset.chain.as_str(),
                "asset": self.asset.asset.as_str(),
            },
            "observable": self.before.is_some() && self.after.is_some(),
            "before": self.before.map(|value| value.to_string()),
            "after": self.after.map(|value| value.to_string()),
            "credit_delta": self.credit_delta().map(|value| value.to_string()),
            "debit_delta": self.debit_delta().map(|value| value.to_string()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BalanceProbeRole {
    Source,
    Destination,
}

fn custody_vault_balance_is_chain_observable(vault: &CustodyVault, asset: &DepositAsset) -> bool {
    match vault.role {
        CustodyVaultRole::HyperliquidSpot => false,
        CustodyVaultRole::SourceDeposit | CustodyVaultRole::DestinationExecution => {
            vault.chain == asset.chain
        }
    }
}

impl BalanceProbeRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Destination => "destination",
        }
    }
}

struct StepCompletion {
    response: serde_json::Value,
    tx_hash: Option<String>,
    outcome: RunningStepOutcome,
}

/// Dispatches `post_execute` for a `CustodyActions` step to whichever
/// provider category owns it. Bridges (Across) derive `deposit_id` from
/// logs; exchanges (Hyperliquid) derive `oid` from the response JSON;
/// units (HyperUnit) flip the operation to `WaitingExternal` so observers
/// begin polling `/operations/{protocol_address}`.
enum PostExecuteProvider {
    Bridge(Arc<dyn BridgeProvider>),
    Exchange(Arc<dyn ExchangeProvider>),
    Unit(Arc<dyn UnitProvider>),
}

impl PostExecuteProvider {
    async fn post_execute(
        &self,
        provider_context: &Value,
        receipts: &[CustodyActionReceipt],
    ) -> Result<ProviderExecutionStatePatch, String> {
        match self {
            Self::Bridge(provider) => provider.post_execute(provider_context, receipts).await,
            Self::Exchange(provider) => provider.post_execute(provider_context, receipts).await,
            Self::Unit(provider) => provider.post_execute(provider_context, receipts).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{MarketOrderAction, RouterOrderAction, RouterOrderType};
    use crate::services::AssetRegistry;

    fn test_asset(chain: &str, asset: AssetId) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).expect("valid chain id"),
            asset,
        }
    }

    fn test_order(
        order_id: Uuid,
        source_asset: DepositAsset,
        destination_asset: DepositAsset,
        refund_address: &str,
    ) -> RouterOrder {
        RouterOrder {
            id: order_id,
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::RefundRequired,
            funding_vault_id: None,
            source_asset,
            destination_asset,
            recipient_address: "bc1qrecipient0000000000000000000000000000000".to_string(),
            refund_address: refund_address.to_string(),
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "1000000".to_string(),
                    min_amount_out: "1".to_string(),
                },
            }),
            action_timeout_at: Utc::now() + chrono::Duration::hours(1),
            idempotency_key: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn quote_asset_json(asset: &DepositAsset) -> Value {
        json!({
            "chain_id": asset.chain.as_str(),
            "asset": asset.asset.as_str(),
        })
    }

    fn refund_trade_leg(
        transition: &TransitionDecl,
        amount_in: &str,
        amount_out: &str,
        min_amount_out: &str,
    ) -> Value {
        json!({
            "transition_parent_decl_id": transition.id,
            "provider": transition.provider.as_str(),
            "amount_in": amount_in,
            "amount_out": amount_out,
            "input_asset": quote_asset_json(&transition.input.asset),
            "output_asset": quote_asset_json(&transition.output.asset),
            "raw": {
                "order_kind": "market",
                "amount_in": amount_in,
                "amount_out": amount_out,
                "min_amount_out": min_amount_out,
                "input_asset": quote_asset_json(&transition.input.asset),
                "output_asset": quote_asset_json(&transition.output.asset),
            }
        })
    }

    fn refund_simple_leg(transition: &TransitionDecl, amount_in: &str, amount_out: &str) -> Value {
        json!({
            "transition_parent_decl_id": transition.id,
            "provider": transition.provider.as_str(),
            "amount_in": amount_in,
            "amount_out": amount_out,
            "input_asset": quote_asset_json(&transition.input.asset),
            "output_asset": quote_asset_json(&transition.output.asset),
        })
    }

    fn test_step(
        order_id: Uuid,
        step_index: i32,
        step_type: OrderExecutionStepType,
        request: Value,
    ) -> OrderExecutionStep {
        OrderExecutionStep {
            id: Uuid::now_v7(),
            order_id,
            execution_attempt_id: None,
            transition_decl_id: None,
            step_index,
            step_type,
            provider: "test".to_string(),
            status: OrderExecutionStepStatus::Planned,
            input_asset: None,
            output_asset: None,
            amount_in: None,
            min_amount_out: None,
            tx_hash: None,
            provider_ref: None,
            idempotency_key: None,
            attempt_count: 0,
            next_attempt_at: None,
            started_at: None,
            completed_at: None,
            details: json!({}),
            request,
            response: json!({}),
            error: json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn balance_probe(
        role: BalanceProbeRole,
        vault_id: Uuid,
        asset: DepositAsset,
        before: u64,
        after: u64,
    ) -> StepBalanceProbe {
        StepBalanceProbe {
            role,
            vault_id,
            address: Some("0x1000000000000000000000000000000000000001".to_string()),
            asset,
            before: Some(U256::from(before)),
            after: Some(U256::from(after)),
        }
    }

    fn test_vault(
        order_id: Uuid,
        role: CustodyVaultRole,
        visibility: CustodyVaultVisibility,
        control_type: CustodyVaultControlType,
        address: &str,
    ) -> CustodyVault {
        CustodyVault {
            id: Uuid::now_v7(),
            order_id: Some(order_id),
            role,
            visibility,
            chain: match role {
                CustodyVaultRole::HyperliquidSpot => {
                    ChainId::parse("hyperliquid").expect("valid chain id")
                }
                _ => ChainId::parse("evm:42161").expect("valid chain id"),
            },
            asset: match role {
                CustodyVaultRole::HyperliquidSpot => None,
                _ => Some(AssetId::Native),
            },
            address: address.to_string(),
            control_type,
            derivation_salt: Some([0x11; 32]),
            signer_ref: None,
            status: CustodyVaultStatus::Active,
            metadata: json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn runtime_custody_validator_accepts_supported_across_to_unit_route() {
        let order_id = Uuid::now_v7();
        let destination_vault = test_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x1000000000000000000000000000000000000001",
        );
        let hyperliquid_vault = test_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x2000000000000000000000000000000000000002",
        );
        let vaults = vec![destination_vault.clone(), hyperliquid_vault.clone()];
        let vaults_by_id: HashMap<Uuid, &CustodyVault> =
            vaults.iter().map(|vault| (vault.id, vault)).collect();
        let steps = vec![
            test_step(
                order_id,
                1,
                OrderExecutionStepType::AcrossBridge,
                json!({
                    "recipient_custody_vault_role": "destination_execution",
                    "recipient_custody_vault_id": destination_vault.id,
                    "recipient": destination_vault.address,
                }),
            ),
            test_step(
                order_id,
                2,
                OrderExecutionStepType::UnitDeposit,
                json!({
                    "source_custody_vault_role": "destination_execution",
                    "source_custody_vault_id": destination_vault.id,
                    "source_custody_vault_address": destination_vault.address,
                    "revert_custody_vault_role": "destination_execution",
                    "revert_custody_vault_id": destination_vault.id,
                    "hyperliquid_custody_vault_role": "hyperliquid_spot",
                    "hyperliquid_custody_vault_id": hyperliquid_vault.id,
                    "hyperliquid_custody_vault_address": hyperliquid_vault.address,
                }),
            ),
            test_step(
                order_id,
                3,
                OrderExecutionStepType::UnitWithdrawal,
                json!({
                    "hyperliquid_custody_vault_role": "hyperliquid_spot",
                    "hyperliquid_custody_vault_id": hyperliquid_vault.id,
                    "hyperliquid_custody_vault_address": hyperliquid_vault.address,
                }),
            ),
        ];

        for (index, step) in steps.iter().enumerate() {
            let is_final = index + 1 == steps.len();
            validate_materialized_intermediate_custody_step(
                order_id,
                step,
                is_final,
                &vaults_by_id,
            )
            .expect("supported route should validate");
        }
    }

    #[test]
    fn balance_observation_for_step_tracks_source_and_internal_destination() {
        let order_id = Uuid::now_v7();
        let source_vault_id = Uuid::now_v7();
        let destination_vault_id = Uuid::now_v7();
        let input_asset = test_asset("evm:8453", AssetId::Native);
        let output_asset = test_asset(
            "evm:8453",
            AssetId::reference("0x4444444444444444444444444444444444444444"),
        );
        let mut step = test_step(
            order_id,
            1,
            OrderExecutionStepType::UniversalRouterSwap,
            json!({
                "recipient_custody_vault_id": destination_vault_id,
            }),
        );
        step.input_asset = Some(input_asset.clone());
        step.output_asset = Some(output_asset.clone());

        let observation = StepBalanceObservation::for_step(&step, source_vault_id);

        assert_eq!(observation.probes.len(), 2);
        assert_eq!(observation.probes[0].role, BalanceProbeRole::Source);
        assert_eq!(observation.probes[0].vault_id, source_vault_id);
        assert_eq!(observation.probes[0].asset, input_asset);
        assert_eq!(observation.probes[1].role, BalanceProbeRole::Destination);
        assert_eq!(observation.probes[1].vault_id, destination_vault_id);
        assert_eq!(observation.probes[1].asset, output_asset);
    }

    #[test]
    fn balance_observation_skips_provider_internal_hyperliquid_vaults() {
        let order_id = Uuid::now_v7();
        let hyperliquid_vault = test_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x2000000000000000000000000000000000000002",
        );
        let bitcoin_asset = test_asset("bitcoin", AssetId::Native);

        assert!(!custody_vault_balance_is_chain_observable(
            &hyperliquid_vault,
            &bitcoin_asset
        ));
    }

    #[test]
    fn balance_observation_only_reads_matching_chain_vaults() {
        let order_id = Uuid::now_v7();
        let destination_vault = test_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x1000000000000000000000000000000000000001",
        );
        let matching_asset = test_asset("evm:42161", AssetId::Native);
        let mismatched_asset = test_asset("bitcoin", AssetId::Native);

        assert!(custody_vault_balance_is_chain_observable(
            &destination_vault,
            &matching_asset
        ));
        assert!(!custody_vault_balance_is_chain_observable(
            &destination_vault,
            &mismatched_asset
        ));
    }

    #[test]
    fn balance_observation_enforces_destination_minimum() {
        let order_id = Uuid::now_v7();
        let destination_vault_id = Uuid::now_v7();
        let output_asset = test_asset(
            "evm:8453",
            AssetId::reference("0x4444444444444444444444444444444444444444"),
        );
        let mut step = test_step(
            order_id,
            1,
            OrderExecutionStepType::UniversalRouterSwap,
            json!({}),
        );
        step.provider = "velora".to_string();
        step.min_amount_out = Some("50".to_string());
        let observation = StepBalanceObservation {
            probes: vec![balance_probe(
                BalanceProbeRole::Destination,
                destination_vault_id,
                output_asset,
                100,
                149,
            )],
        };

        let err = observation
            .enforce_output_minimum(&step, &step.provider)
            .expect_err("destination credit below min should fail");

        assert!(err.to_string().contains("below min_amount_out"), "{err}");
    }

    #[test]
    fn balance_observation_accepts_destination_delta_at_minimum() {
        let order_id = Uuid::now_v7();
        let destination_vault_id = Uuid::now_v7();
        let output_asset = test_asset(
            "evm:8453",
            AssetId::reference("0x4444444444444444444444444444444444444444"),
        );
        let mut step = test_step(
            order_id,
            1,
            OrderExecutionStepType::UniversalRouterSwap,
            json!({ "min_amount_out": "50" }),
        );
        step.provider = "velora".to_string();
        let observation = StepBalanceObservation {
            probes: vec![balance_probe(
                BalanceProbeRole::Destination,
                destination_vault_id,
                output_asset,
                100,
                150,
            )],
        };

        observation
            .enforce_output_minimum(&step, &step.provider)
            .expect("destination credit at min should pass");
    }

    #[test]
    fn runtime_custody_validator_rejects_across_recipient_address_mismatch() {
        let order_id = Uuid::now_v7();
        let destination_vault = test_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x1000000000000000000000000000000000000001",
        );
        let vaults = vec![destination_vault.clone()];
        let vaults_by_id: HashMap<Uuid, &CustodyVault> =
            vaults.iter().map(|vault| (vault.id, vault)).collect();
        let step = test_step(
            order_id,
            1,
            OrderExecutionStepType::AcrossBridge,
            json!({
                "recipient_custody_vault_role": "destination_execution",
                "recipient_custody_vault_id": destination_vault.id,
                "recipient": "0x9999999999999999999999999999999999999999",
            }),
        );

        let err =
            validate_materialized_intermediate_custody_step(order_id, &step, false, &vaults_by_id)
                .expect_err("address mismatch must reject");
        assert!(matches!(
            err,
            OrderExecutionError::IntermediateCustodyInvariant { .. }
        ));
    }

    #[test]
    fn runtime_custody_validator_accepts_nonfinal_unit_withdrawal_to_internal_custody() {
        let order_id = Uuid::now_v7();
        let hyperliquid_vault = test_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x2000000000000000000000000000000000000002",
        );
        let destination_vault = test_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x3000000000000000000000000000000000000003",
        );
        let vaults = vec![hyperliquid_vault.clone(), destination_vault.clone()];
        let vaults_by_id: HashMap<Uuid, &CustodyVault> =
            vaults.iter().map(|vault| (vault.id, vault)).collect();
        let step = test_step(
            order_id,
            1,
            OrderExecutionStepType::UnitWithdrawal,
            json!({
                "recipient_custody_vault_role": "destination_execution",
                "recipient_custody_vault_id": destination_vault.id,
                "recipient_address": destination_vault.address,
                "hyperliquid_custody_vault_role": "hyperliquid_spot",
                "hyperliquid_custody_vault_id": hyperliquid_vault.id,
                "hyperliquid_custody_vault_address": hyperliquid_vault.address,
            }),
        );

        validate_materialized_intermediate_custody_step(order_id, &step, false, &vaults_by_id)
            .expect("non-final unit withdrawal to internal custody must validate");
    }

    #[test]
    fn runtime_custody_validator_rejects_nonfinal_unit_withdrawal_without_internal_recipient() {
        let order_id = Uuid::now_v7();
        let hyperliquid_vault = test_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x2000000000000000000000000000000000000002",
        );
        let vaults = vec![hyperliquid_vault.clone()];
        let vaults_by_id: HashMap<Uuid, &CustodyVault> =
            vaults.iter().map(|vault| (vault.id, vault)).collect();
        let step = test_step(
            order_id,
            1,
            OrderExecutionStepType::UnitWithdrawal,
            json!({
                "hyperliquid_custody_vault_role": "hyperliquid_spot",
                "hyperliquid_custody_vault_id": hyperliquid_vault.id,
                "hyperliquid_custody_vault_address": hyperliquid_vault.address,
            }),
        );

        let err =
            validate_materialized_intermediate_custody_step(order_id, &step, false, &vaults_by_id)
                .expect_err("missing non-final unit withdrawal recipient must reject");
        assert!(matches!(
            err,
            OrderExecutionError::IntermediateCustodyInvariant { .. }
        ));
    }

    #[test]
    fn refund_materializer_builds_external_to_btc_refund_path_through_unit_and_hyperliquid() {
        let order_id = Uuid::now_v7();
        let source_asset = test_asset("bitcoin", AssetId::Native);
        let destination_asset = test_asset("evm:8453", AssetId::Reference("usdc".to_string()));
        let order = test_order(
            order_id,
            source_asset,
            destination_asset,
            "bc1qrefund0000000000000000000000000000000",
        );
        let source_vault = test_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x4000000000000000000000000000000000000004",
        );
        let position = RefundSourcePosition {
            handle: RefundSourceHandle::ExternalCustody(source_vault.clone()),
            asset: test_asset("evm:1", AssetId::Native),
            amount: "1000000000000000000".to_string(),
        };

        let registry = AssetRegistry::default();
        let path = registry
            .select_transition_paths_between(
                MarketOrderNode::External(position.asset.clone()),
                MarketOrderNode::External(order.source_asset.clone()),
                5,
            )
            .into_iter()
            .find(|path| {
                path.transitions
                    .iter()
                    .map(|transition| transition.kind)
                    .collect::<Vec<_>>()
                    == vec![
                        MarketOrderTransitionKind::UnitDeposit,
                        MarketOrderTransitionKind::HyperliquidTrade,
                        MarketOrderTransitionKind::HyperliquidTrade,
                        MarketOrderTransitionKind::UnitWithdrawal,
                    ]
            })
            .expect("expected ETH -> BTC refund path through unit and hyperliquid");

        let quoted_path = RefundQuotedPath {
            path: path.clone(),
            amount_out: "75000".to_string(),
            legs: vec![
                refund_simple_leg(
                    &path.transitions[0],
                    "1000000000000000000",
                    "1000000000000000000",
                ),
                refund_trade_leg(
                    &path.transitions[1],
                    "1000000000000000000",
                    "5000000000",
                    "4900000000",
                ),
                refund_trade_leg(&path.transitions[2], "5000000000", "75000", "74000"),
                refund_simple_leg(&path.transitions[3], "75000", "74674"),
            ],
        };

        let steps = materialize_refund_transition_steps(
            &order,
            &position,
            &quoted_path,
            Uuid::now_v7(),
            Utc::now(),
        )
        .expect("refund path should materialize");

        assert_eq!(steps.len(), 4);
        assert_eq!(
            steps.iter().map(|step| step.step_type).collect::<Vec<_>>(),
            vec![
                OrderExecutionStepType::UnitDeposit,
                OrderExecutionStepType::HyperliquidTrade,
                OrderExecutionStepType::HyperliquidTrade,
                OrderExecutionStepType::UnitWithdrawal,
            ]
        );
        assert_eq!(
            steps[0].transition_decl_id,
            Some(path.transitions[0].id.clone())
        );
        assert_eq!(
            steps[0].request["source_custody_vault_id"],
            json!(source_vault.id)
        );
        assert_eq!(
            steps[0].request["source_custody_vault_role"],
            json!(CustodyVaultRole::DestinationExecution.to_db_string())
        );
        assert_eq!(
            steps[1].request["hyperliquid_custody_vault_role"],
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string())
        );
        assert_eq!(
            steps[2].request["hyperliquid_custody_vault_role"],
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string())
        );
        assert_eq!(
            steps[3].request["recipient_address"],
            json!(order.refund_address)
        );
        assert_eq!(
            steps[3].request["hyperliquid_custody_vault_role"],
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string())
        );
    }

    #[test]
    fn refund_materializer_builds_hyperliquid_to_source_path_via_unit_and_across() {
        let order_id = Uuid::now_v7();
        let source_asset = test_asset("evm:8453", AssetId::Native);
        let destination_asset = test_asset("bitcoin", AssetId::Native);
        let order = test_order(
            order_id,
            source_asset,
            destination_asset,
            "0x5000000000000000000000000000000000000005",
        );
        let hyperliquid_vault = test_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            CustodyVaultControlType::RouterDerivedKey,
            "0x6000000000000000000000000000000000000006",
        );
        let position = RefundSourcePosition {
            handle: RefundSourceHandle::HyperliquidSpot {
                vault: hyperliquid_vault.clone(),
                coin: "USDC".to_string(),
                canonical: CanonicalAsset::Usdc,
            },
            asset: test_asset("hyperliquid", AssetId::Native),
            amount: "1000000".to_string(),
        };

        let registry = AssetRegistry::default();
        let path = registry
            .select_transition_paths_between(
                MarketOrderNode::Venue {
                    provider: ProviderId::Hyperliquid,
                    canonical: CanonicalAsset::Usdc,
                },
                MarketOrderNode::External(order.source_asset.clone()),
                5,
            )
            .into_iter()
            .find(|path| {
                path.transitions
                    .iter()
                    .map(|transition| transition.kind)
                    .collect::<Vec<_>>()
                    == vec![
                        MarketOrderTransitionKind::HyperliquidTrade,
                        MarketOrderTransitionKind::UnitWithdrawal,
                        MarketOrderTransitionKind::AcrossBridge,
                    ]
            })
            .expect("expected Hyperliquid -> Base ETH refund path through unit and across");

        let quoted_path = RefundQuotedPath {
            path: path.clone(),
            amount_out: "490000000000000".to_string(),
            legs: vec![
                refund_trade_leg(
                    &path.transitions[0],
                    "1000000",
                    "500000000000000",
                    "490000000000000",
                ),
                refund_simple_leg(&path.transitions[1], "500000000000000", "495000000000000"),
                refund_simple_leg(&path.transitions[2], "495000000000000", "490000000000000"),
            ],
        };

        let steps = materialize_refund_transition_steps(
            &order,
            &position,
            &quoted_path,
            Uuid::now_v7(),
            Utc::now(),
        )
        .expect("refund path should materialize");

        assert_eq!(steps.len(), 3);
        assert_eq!(
            steps.iter().map(|step| step.step_type).collect::<Vec<_>>(),
            vec![
                OrderExecutionStepType::HyperliquidTrade,
                OrderExecutionStepType::UnitWithdrawal,
                OrderExecutionStepType::AcrossBridge,
            ]
        );
        assert_eq!(
            steps[0].request["hyperliquid_custody_vault_id"],
            json!(hyperliquid_vault.id)
        );
        assert_eq!(
            steps[1].request["recipient_custody_vault_role"],
            json!(CustodyVaultRole::DestinationExecution.to_db_string())
        );
        assert_eq!(
            steps[1].request["hyperliquid_custody_vault_id"],
            json!(hyperliquid_vault.id)
        );
        assert_eq!(
            steps[2].request["depositor_custody_vault_role"],
            json!(CustodyVaultRole::DestinationExecution.to_db_string())
        );
        assert_eq!(
            steps[2].request["refund_custody_vault_role"],
            json!(CustodyVaultRole::DestinationExecution.to_db_string())
        );
        assert_eq!(steps[2].request["recipient"], json!(order.refund_address));
    }
}

impl From<OrderExecutionError> for RouterServerError {
    fn from(err: OrderExecutionError) -> Self {
        match err {
            OrderExecutionError::Database { source } => source,
            source => Self::Internal {
                message: source.to_string(),
            },
        }
    }
}
