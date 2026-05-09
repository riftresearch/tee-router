use std::{error::Error as StdError, fmt::Display};

use serde::{Deserialize, Serialize};
use snafu::Snafu;
use temporalio_client::{
    Client, ClientOptions, UntypedSignal, WorkflowHandle, WorkflowSignalOptions,
    WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::{PayloadConverter, RawValue},
    protos::temporal::api::enums::v1::{WorkflowIdConflictPolicy, WorkflowIdReusePolicy},
    UntypedWorkflow,
};
use uuid::Uuid;

pub const DEFAULT_TASK_QUEUE: &str = "tee-router-order-execution";
pub const ORDER_WORKFLOW_TYPE: &str = "OrderWorkflow";
pub const ORDER_WORKFLOW_PROVIDER_HINT_SIGNAL: &str = "provider_operation_hint";

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub struct TemporalConnection {
    pub temporal_address: String,
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWorkflowInput {
    pub order_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintSignal {
    pub order_id: Uuid,
    pub hint_id: Uuid,
    pub provider_operation_id: Option<Uuid>,
    pub provider: ProviderKind,
    pub hint_kind: ProviderHintKind,
    pub provider_ref: Option<String>,
    #[serde(default)]
    pub evidence: Option<ProviderOperationHintEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintEvidence {
    pub tx_hash: String,
    pub address: String,
    pub transfer_index: u64,
    #[serde(default)]
    pub amount: Option<String>,
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
}

#[derive(Debug, Snafu)]
pub enum RouterTemporalError {
    #[snafu(display("invalid Temporal address '{address}'"))]
    InvalidTemporalAddress {
        address: String,
        source: url::ParseError,
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
    client: Client,
    task_queue: String,
}

impl OrderWorkflowClient {
    #[must_use]
    pub fn new(client: Client, task_queue: impl Into<String>) -> Self {
        Self {
            client,
            task_queue: task_queue.into(),
        }
    }

    pub async fn connect(
        connection: &TemporalConnection,
        task_queue: impl Into<String>,
    ) -> RouterTemporalResult<Self> {
        let client = connect_client(connection).await?;
        Ok(Self::new(client, task_queue))
    }

    pub async fn start_order_workflow(
        &self,
        order_id: Uuid,
    ) -> RouterTemporalResult<WorkflowHandle<Client, UntypedWorkflow>> {
        let workflow_id = order_workflow_id(order_id);
        let input = raw_workflow_value(&OrderWorkflowInput { order_id });
        self.client
            .start_workflow(
                UntypedWorkflow::new(ORDER_WORKFLOW_TYPE),
                input,
                workflow_start_options(&self.task_queue, &workflow_id),
            )
            .await
            .map_err(|source| RouterTemporalError::Temporal {
                action: "start OrderWorkflow",
                source: boxed(source),
            })
    }

    pub async fn signal_provider_hint(
        &self,
        order_id: Uuid,
        signal: ProviderOperationHintSignal,
    ) -> RouterTemporalResult<()> {
        let workflow_id = order_workflow_id(order_id);
        let input = raw_workflow_value(&signal);
        let handle = self
            .client
            .get_workflow_handle::<UntypedWorkflow>(workflow_id);
        handle
            .signal(
                UntypedSignal::new(ORDER_WORKFLOW_PROVIDER_HINT_SIGNAL),
                input,
                WorkflowSignalOptions::default(),
            )
            .await
            .map_err(|source| RouterTemporalError::Temporal {
                action: "signal OrderWorkflow provider-operation hint",
                source: boxed(source),
            })
    }
}

pub async fn connect_client(connection: &TemporalConnection) -> RouterTemporalResult<Client> {
    let target =
        temporalio_sdk_core::Url::parse(&connection.temporal_address).map_err(|source| {
            RouterTemporalError::InvalidTemporalAddress {
                address: connection.temporal_address.clone(),
                source,
            }
        })?;
    let connection_options = temporalio_client::ConnectionOptions::new(target).build();
    let temporal_connection = temporalio_client::Connection::connect(connection_options)
        .await
        .map_err(|source| RouterTemporalError::Temporal {
            action: "connect to Temporal",
            source: boxed(source),
        })?;

    Client::new(
        temporal_connection,
        ClientOptions::new(connection.namespace.clone()).build(),
    )
    .map_err(|source| RouterTemporalError::Temporal {
        action: "create Temporal client",
        source: boxed(source),
    })
}

#[must_use]
pub fn order_workflow_id(order_id: Uuid) -> String {
    format!("order:{order_id}:execution")
}

#[must_use]
pub fn refund_workflow_id(order_id: Uuid, parent_attempt_id: Uuid) -> String {
    format!("order:{order_id}:refund:{parent_attempt_id}")
}

#[must_use]
pub fn quote_refresh_workflow_id(order_id: Uuid, failed_step_id: Uuid) -> String {
    format!("order:{order_id}:quote-refresh:{failed_step_id}")
}

#[must_use]
pub fn provider_hint_poll_workflow_id(order_id: Uuid, step_id: Uuid) -> String {
    format!("order:{order_id}:provider-hint-poll:{step_id}")
}

#[must_use]
pub fn workflow_start_options(task_queue: &str, workflow_id: &str) -> WorkflowStartOptions {
    WorkflowStartOptions::new(task_queue.to_owned(), workflow_id.to_owned())
        .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicateFailedOnly)
        .id_conflict_policy(WorkflowIdConflictPolicy::Fail)
        .build()
}

#[must_use]
pub fn raw_workflow_value<T: Serialize + 'static>(value: &T) -> RawValue {
    RawValue::from_value(value, &PayloadConverter::default())
}

pub fn boxed(source: impl Display) -> BoxError {
    Box::new(std::io::Error::other(source.to_string()))
}
