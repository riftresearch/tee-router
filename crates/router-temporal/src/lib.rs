use std::{error::Error as StdError, fmt::Display};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use temporalio_common::{
    data_converters::{PayloadConverter, RawValue},
    protos::temporal::api::{
        common::v1::{Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{TaskQueueKind, WorkflowIdConflictPolicy, WorkflowIdReusePolicy},
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{
            workflow_service_client::WorkflowServiceClient, SignalWorkflowExecutionRequest,
            StartWorkflowExecutionRequest,
        },
    },
};
use tonic::{transport::Channel, Code};
use uuid::Uuid;

pub const DEFAULT_TASK_QUEUE: &str = "tee-router-order-execution";
pub const ORDER_WORKFLOW_TYPE: &str = "OrderWorkflow";
pub const ORDER_WORKFLOW_PROVIDER_HINT_SIGNAL: &str = "provider_operation_hint";
pub const ORDER_WORKFLOW_MANUAL_RELEASE_SIGNAL: &str = "manual_intervention_release";
pub const ORDER_WORKFLOW_MANUAL_TRIGGER_REFUND_SIGNAL: &str = "manual_refund_trigger";
pub const ORDER_WORKFLOW_ACKNOWLEDGE_UNRECOVERABLE_SIGNAL: &str = "acknowledge_unrecoverable";

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

macro_rules! workflow_id_type {
    ($name:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            #[must_use]
            pub fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            #[must_use]
            pub fn inner(self) -> Uuid {
                self.0
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl PartialEq<Uuid> for $name {
            fn eq(&self, other: &Uuid) -> bool {
                self.0 == *other
            }
        }

        impl PartialEq<$name> for Uuid {
            fn eq(&self, other: &$name) -> bool {
                *self == other.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

workflow_id_type!(WorkflowOrderId);
workflow_id_type!(WorkflowAttemptId);
workflow_id_type!(WorkflowStepId);
workflow_id_type!(WorkflowVaultId);
workflow_id_type!(WorkflowProviderOperationId);
workflow_id_type!(WorkflowHintId);

/// Compile-fail examples proving workflow IDs are not interchangeable.
///
/// ```compile_fail
/// use router_temporal::{WorkflowAttemptId, WorkflowOrderId};
/// let attempt_id = WorkflowAttemptId::from(uuid::Uuid::nil());
/// let _: WorkflowOrderId = attempt_id;
/// ```
///
/// ```compile_fail
/// use router_temporal::{WorkflowAttemptId, WorkflowStepId};
/// let step_id = WorkflowStepId::from(uuid::Uuid::nil());
/// let _: WorkflowAttemptId = step_id;
/// ```
///
/// ```compile_fail
/// use router_temporal::{WorkflowStepId, WorkflowVaultId};
/// let vault_id = WorkflowVaultId::from(uuid::Uuid::nil());
/// let _: WorkflowStepId = vault_id;
/// ```
///
/// ```compile_fail
/// use router_temporal::{WorkflowProviderOperationId, WorkflowVaultId};
/// let provider_operation_id = WorkflowProviderOperationId::from(uuid::Uuid::nil());
/// let _: WorkflowVaultId = provider_operation_id;
/// ```
///
/// ```compile_fail
/// use router_temporal::{WorkflowHintId, WorkflowProviderOperationId};
/// let hint_id = WorkflowHintId::from(uuid::Uuid::nil());
/// let _: WorkflowProviderOperationId = hint_id;
/// ```
///
/// ```compile_fail
/// use router_temporal::{WorkflowHintId, WorkflowOrderId};
/// let order_id = WorkflowOrderId::from(uuid::Uuid::nil());
/// let _: WorkflowHintId = order_id;
/// ```
#[doc(hidden)]
pub mod workflow_id_type_safety {}

#[derive(Debug, Clone)]
pub struct TemporalConnection {
    pub temporal_address: String,
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWorkflowInput {
    pub order_id: WorkflowOrderId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintSignal {
    pub order_id: WorkflowOrderId,
    pub execution_step_id: WorkflowStepId,
    pub hint_id: WorkflowHintId,
    pub provider_operation_id: Option<WorkflowProviderOperationId>,
    pub provider: ProviderKind,
    pub hint_kind: ProviderHintKind,
    pub provider_ref: Option<String>,
    #[serde(default)]
    pub evidence: Option<ProviderOperationHintEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProviderOperationHintEvidence {
    UnitDeposit(UnitDepositHintEvidence),
    HlTradeFilled(HlTradeFilledEvidence),
    HlTradeCanceled(HlTradeCanceledEvidence),
    HlBridgeDepositObserved(HlBridgeDepositObservedEvidence),
    HlBridgeDepositCredited(HlBridgeDepositCreditedEvidence),
    HlWithdrawalAcknowledged(HlWithdrawalAcknowledgedEvidence),
    HlWithdrawalSettled(HlWithdrawalSettledEvidence),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnitDepositHintEvidence {
    pub tx_hash: String,
    pub address: String,
    pub transfer_index: u64,
    #[serde(default)]
    pub amount: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HlTradeFilledEvidence {
    pub user: String,
    pub oid: u64,
    pub tid: u64,
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub crossed: bool,
    pub hash: String,
    pub time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HlTradeCanceledEvidence {
    pub user: String,
    pub oid: u64,
    pub coin: String,
    pub status: String,
    pub status_timestamp_ms: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HlBridgeDepositObservedEvidence {
    pub user: String,
    pub usdc: String,
    pub arb_tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HlBridgeDepositCreditedEvidence {
    pub user: String,
    pub usdc: String,
    pub hl_credit_hash: String,
    pub hl_credit_time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HlWithdrawalAcknowledgedEvidence {
    pub user: String,
    pub usdc: String,
    pub nonce: u64,
    pub hl_request_hash: String,
    pub hl_request_time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HlWithdrawalSettledEvidence {
    pub user: String,
    pub usdc: String,
    pub arb_payout_tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub time_window_match_to_nonce: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualReleaseSignal {
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_id: Option<String>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualTriggerRefundSignal {
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_id: Option<String>,
    pub requested_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refund_kind_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcknowledgeUnrecoverableSignal {
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_id: Option<String>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderKind {
    Bridge,
    Unit,
    Exchange,
    CustodyActionExecutor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderHintKind {
    CctpAttestation,
    AcrossFill,
    UnitDeposit,
    ProviderObservation,
    HyperliquidTrade,
    HlTradeFilled,
    HlTradeCanceled,
    HlBridgeDepositObserved,
    HlBridgeDepositCredited,
    HlWithdrawalAcknowledged,
    HlWithdrawalSettled,
}

#[derive(Debug, Snafu)]
pub enum RouterTemporalError {
    #[snafu(display("invalid Temporal address '{address}'"))]
    InvalidTemporalAddress {
        address: String,
        source: url::ParseError,
    },

    #[snafu(display("OrderWorkflow {workflow_id} already started"))]
    WorkflowAlreadyStarted {
        workflow_id: String,
        run_id: Option<String>,
    },

    #[snafu(display("workflow {workflow_id} is unavailable for signal: {message}"))]
    WorkflowSignalUnavailable {
        workflow_id: String,
        message: String,
    },

    #[snafu(display("failed to {action}"))]
    Temporal {
        action: &'static str,
        source: BoxError,
    },
}

pub type RouterTemporalResult<T> = Result<T, RouterTemporalError>;

#[derive(Clone)]
pub struct OrderWorkflowClient {
    client: WorkflowServiceClient<Channel>,
    namespace: String,
    task_queue: String,
    identity: String,
}

impl OrderWorkflowClient {
    #[must_use]
    pub fn new(
        client: WorkflowServiceClient<Channel>,
        namespace: impl Into<String>,
        task_queue: impl Into<String>,
    ) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            task_queue: task_queue.into(),
            identity: "tee-router".to_owned(),
        }
    }

    pub async fn connect(
        connection: &TemporalConnection,
        task_queue: impl Into<String>,
    ) -> RouterTemporalResult<Self> {
        let client = connect_client(connection).await?;
        Ok(Self::new(
            client,
            connection.namespace.clone(),
            task_queue.into(),
        ))
    }

    pub async fn start_order_workflow(
        &self,
        order_id: WorkflowOrderId,
    ) -> RouterTemporalResult<String> {
        let workflow_id = order_workflow_id(order_id);
        let input = payloads(&OrderWorkflowInput { order_id });
        let response = self
            .client
            .clone()
            .start_workflow_execution(StartWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                workflow_id: workflow_id.clone(),
                workflow_type: Some(WorkflowType {
                    name: ORDER_WORKFLOW_TYPE.to_owned(),
                }),
                task_queue: Some(TaskQueue {
                    name: self.task_queue.clone(),
                    kind: TaskQueueKind::Unspecified as i32,
                    normal_name: String::new(),
                }),
                input: Some(input),
                identity: self.identity.clone(),
                request_id: Uuid::now_v7().to_string(),
                workflow_id_reuse_policy: WorkflowIdReusePolicy::AllowDuplicateFailedOnly as i32,
                workflow_id_conflict_policy: WorkflowIdConflictPolicy::Fail as i32,
                ..Default::default()
            })
            .await
            .map_err(|source| {
                if source.code() == Code::AlreadyExists {
                    RouterTemporalError::WorkflowAlreadyStarted {
                        workflow_id: workflow_id.clone(),
                        run_id: None,
                    }
                } else {
                    RouterTemporalError::Temporal {
                        action: "start OrderWorkflow",
                        source: boxed(source),
                    }
                }
            })?;
        Ok(response.into_inner().run_id)
    }

    pub async fn signal_provider_hint(
        &self,
        order_id: WorkflowOrderId,
        signal: ProviderOperationHintSignal,
    ) -> RouterTemporalResult<()> {
        let workflow_id = order_workflow_id(order_id);
        self.signal_provider_hint_to_workflow(workflow_id, signal)
            .await
    }

    pub async fn signal_manual_release(
        &self,
        order_id: WorkflowOrderId,
        signal: ManualReleaseSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_manual_release_to_workflow(order_workflow_id(order_id), signal)
            .await
    }

    pub async fn signal_refund_manual_release(
        &self,
        order_id: WorkflowOrderId,
        parent_attempt_id: WorkflowAttemptId,
        signal: ManualReleaseSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_manual_release_to_workflow(
            refund_workflow_id(order_id, parent_attempt_id),
            signal,
        )
        .await
    }

    pub async fn signal_manual_trigger_refund(
        &self,
        order_id: WorkflowOrderId,
        signal: ManualTriggerRefundSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_manual_trigger_refund_to_workflow(order_workflow_id(order_id), signal)
            .await
    }

    pub async fn signal_refund_manual_trigger_refund(
        &self,
        order_id: WorkflowOrderId,
        parent_attempt_id: WorkflowAttemptId,
        signal: ManualTriggerRefundSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_manual_trigger_refund_to_workflow(
            refund_workflow_id(order_id, parent_attempt_id),
            signal,
        )
        .await
    }

    pub async fn signal_acknowledge_unrecoverable(
        &self,
        order_id: WorkflowOrderId,
        signal: AcknowledgeUnrecoverableSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_acknowledge_unrecoverable_to_workflow(order_workflow_id(order_id), signal)
            .await
    }

    pub async fn signal_refund_acknowledge_unrecoverable(
        &self,
        order_id: WorkflowOrderId,
        parent_attempt_id: WorkflowAttemptId,
        signal: AcknowledgeUnrecoverableSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_acknowledge_unrecoverable_to_workflow(
            refund_workflow_id(order_id, parent_attempt_id),
            signal,
        )
        .await
    }

    async fn signal_manual_release_to_workflow(
        &self,
        workflow_id: String,
        signal: ManualReleaseSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_workflow(
            workflow_id,
            ORDER_WORKFLOW_MANUAL_RELEASE_SIGNAL,
            &signal,
            "signal manual-intervention release workflow",
        )
        .await
    }

    async fn signal_manual_trigger_refund_to_workflow(
        &self,
        workflow_id: String,
        signal: ManualTriggerRefundSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_workflow(
            workflow_id,
            ORDER_WORKFLOW_MANUAL_TRIGGER_REFUND_SIGNAL,
            &signal,
            "signal manual refund workflow",
        )
        .await
    }

    async fn signal_acknowledge_unrecoverable_to_workflow(
        &self,
        workflow_id: String,
        signal: AcknowledgeUnrecoverableSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_workflow(
            workflow_id,
            ORDER_WORKFLOW_ACKNOWLEDGE_UNRECOVERABLE_SIGNAL,
            &signal,
            "signal acknowledge-unrecoverable workflow",
        )
        .await
    }

    pub async fn signal_refund_provider_hint(
        &self,
        order_id: WorkflowOrderId,
        parent_attempt_id: WorkflowAttemptId,
        signal: ProviderOperationHintSignal,
    ) -> RouterTemporalResult<()> {
        let workflow_id = refund_workflow_id(order_id, parent_attempt_id);
        self.signal_provider_hint_to_workflow(workflow_id, signal)
            .await
    }

    async fn signal_provider_hint_to_workflow(
        &self,
        workflow_id: String,
        signal: ProviderOperationHintSignal,
    ) -> RouterTemporalResult<()> {
        self.signal_workflow(
            workflow_id,
            ORDER_WORKFLOW_PROVIDER_HINT_SIGNAL,
            &signal,
            "signal provider-operation hint workflow",
        )
        .await
    }

    async fn signal_workflow<T: Serialize + 'static>(
        &self,
        workflow_id: String,
        signal_name: &'static str,
        signal: &T,
        action: &'static str,
    ) -> RouterTemporalResult<()> {
        let input = payloads(signal);
        self.client
            .clone()
            .signal_workflow_execution(SignalWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: workflow_id.clone(),
                    run_id: String::new(),
                }),
                signal_name: signal_name.to_owned(),
                input: Some(input),
                identity: self.identity.clone(),
                request_id: Uuid::now_v7().to_string(),
                ..Default::default()
            })
            .await
            .map_err(|source| match source.code() {
                Code::NotFound | Code::FailedPrecondition => {
                    RouterTemporalError::WorkflowSignalUnavailable {
                        workflow_id: workflow_id.clone(),
                        message: source.message().to_owned(),
                    }
                }
                _ => RouterTemporalError::Temporal {
                    action,
                    source: boxed(source),
                },
            })?;
        Ok(())
    }
}

pub async fn connect_client(
    connection: &TemporalConnection,
) -> RouterTemporalResult<WorkflowServiceClient<Channel>> {
    url::Url::parse(&connection.temporal_address).map_err(|source| {
        RouterTemporalError::InvalidTemporalAddress {
            address: connection.temporal_address.clone(),
            source,
        }
    })?;
    WorkflowServiceClient::connect(connection.temporal_address.clone())
        .await
        .map_err(|source| RouterTemporalError::Temporal {
            action: "connect to Temporal",
            source: boxed(source),
        })
}

#[must_use]
pub fn order_workflow_id(order_id: WorkflowOrderId) -> String {
    format!("order:{order_id}:execution")
}

#[must_use]
pub fn refund_workflow_id(
    order_id: WorkflowOrderId,
    parent_attempt_id: WorkflowAttemptId,
) -> String {
    format!("order:{order_id}:refund:{parent_attempt_id}")
}

#[must_use]
pub fn quote_refresh_workflow_id(
    order_id: WorkflowOrderId,
    failed_step_id: WorkflowStepId,
) -> String {
    format!("order:{order_id}:quote-refresh:{failed_step_id}")
}

#[must_use]
pub fn raw_workflow_value<T: Serialize + 'static>(value: &T) -> RawValue {
    RawValue::from_value(value, &PayloadConverter::default())
}

#[must_use]
fn payloads<T: Serialize + 'static>(value: &T) -> Payloads {
    Payloads {
        payloads: raw_workflow_value(value).payloads,
    }
}

pub fn boxed(source: impl Display) -> BoxError {
    Box::new(std::io::Error::other(source.to_string()))
}
