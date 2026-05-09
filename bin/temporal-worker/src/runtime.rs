use std::{error::Error as StdError, fmt::Display, str::FromStr};

use snafu::Snafu;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

pub type WorkerResult<T> = std::result::Result<T, WorkerError>;
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Clone)]
pub struct TemporalConnection {
    pub temporal_address: String,
    pub namespace: String,
}

#[derive(Debug, Snafu)]
pub enum WorkerError {
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

    #[snafu(display("worker task failed"))]
    WorkerTask { source: tokio::task::JoinError },

    #[snafu(display("Temporal spike timed out after {seconds}s"))]
    SpikeTimedOut { seconds: u64 },

    #[snafu(display("duplicate workflow ID unexpectedly started: {workflow_id}"))]
    DuplicateWorkflowStarted { workflow_id: String },

    #[snafu(display("Temporal spike assertion failed: {reason}"))]
    Assertion { reason: String },

    #[snafu(display("invalid temporal-worker configuration: {message}"))]
    Configuration { message: String },
}

pub async fn connect_client(connection: &TemporalConnection) -> WorkerResult<Client> {
    let target = Url::from_str(&connection.temporal_address).map_err(|source| {
        WorkerError::InvalidTemporalAddress {
            address: connection.temporal_address.clone(),
            source,
        }
    })?;
    let connection_options = ConnectionOptions::new(target).build();
    let temporal_connection = Connection::connect(connection_options)
        .await
        .map_err(|source| WorkerError::Temporal {
            action: "connect to Temporal",
            source: boxed(source),
        })?;

    Client::new(
        temporal_connection,
        ClientOptions::new(connection.namespace.clone()).build(),
    )
    .map_err(|source| WorkerError::Temporal {
        action: "create Temporal client",
        source: boxed(source),
    })
}

pub fn new_core_runtime() -> WorkerResult<CoreRuntime> {
    let runtime_options = RuntimeOptions::builder()
        .telemetry_options(TelemetryOptions::builder().build())
        .build()
        .map_err(|source| WorkerError::Temporal {
            action: "build Temporal runtime options",
            source: boxed(source),
        })?;

    CoreRuntime::new_assume_tokio(runtime_options).map_err(|source| WorkerError::Temporal {
        action: "create Temporal runtime",
        source: boxed(source),
    })
}

pub fn boxed(source: impl Display) -> BoxError {
    Box::new(std::io::Error::other(source.to_string()))
}
