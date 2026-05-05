use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to connect to replica database"))]
    ReplicaDatabaseConnection { source: sqlx_core::Error },

    #[snafu(display("Failed to connect to Sauron state database"))]
    StateDatabaseConnection { source: sqlx_core::Error },

    #[snafu(display("Failed to apply Sauron state migrations"))]
    StateMigration {
        source: sqlx_core::migrate::MigrateError,
    },

    #[snafu(display("Replica database query failed"))]
    ReplicaDatabaseQuery { source: sqlx_core::Error },

    #[snafu(display("Sauron state database query failed"))]
    StateDatabaseQuery { source: sqlx_core::Error },

    #[snafu(display("Postgres CDC stream failed"))]
    CdcStream {
        source: pgwire_replication::PgWireError,
    },

    #[snafu(display("Failed to parse Postgres CDC payload: {source}"))]
    CdcPayload { source: serde_json::Error },

    #[snafu(display("Postgres CDC payload exceeded {max_bytes} bytes"))]
    CdcPayloadTooLarge { max_bytes: usize },

    #[snafu(display("Postgres CDC payload was invalid: {message}"))]
    InvalidCdcPayload { message: String },

    #[snafu(display("Postgres CDC pending refresh batch exceeded {max_ids} ids"))]
    CdcPendingBatchTooLarge { max_ids: usize },

    #[snafu(display("Replica watch row was invalid: {message}"))]
    InvalidWatchRow { message: String },

    #[snafu(display("Replica cursor row was invalid: {message}"))]
    InvalidCursorRow { message: String },

    #[snafu(display("Sauron configuration is invalid: {message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("Failed to initialize chain {chain}: {message}"))]
    ChainInit { chain: String, message: String },

    #[snafu(display("Failed to initialize discovery backend {backend}: {message}"))]
    DiscoveryBackendInit { backend: String, message: String },

    #[snafu(display("Bitcoin esplora request failed"))]
    BitcoinEsplora { source: esplora_client::Error },

    #[snafu(display("Bitcoin RPC request failed"))]
    BitcoinRpc {
        source: bitcoincore_rpc_async::Error,
    },

    #[snafu(display("EVM RPC request failed: {source}"))]
    EvmRpc {
        source: alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    },

    #[snafu(display(
        "EVM log scan request failed for blocks {from_height}..={to_height}: {source}"
    ))]
    EvmLogScan {
        from_height: u64,
        to_height: u64,
        source: alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    },

    #[snafu(display("EVM token indexer request failed"))]
    EvmTokenIndexer {
        source: evm_token_indexer_client::Error,
    },

    #[snafu(display("ROUTER request failed"))]
    RouterRequest { source: reqwest::Error },

    #[snafu(display("ROUTER response body exceeded {max_bytes} bytes"))]
    RouterResponseBodyTooLarge { max_bytes: usize },

    #[snafu(display("ROUTER returned an invalid JSON body: {source}; body={body}"))]
    RouterResponseJson {
        source: serde_json::Error,
        body: String,
    },

    #[snafu(display("ROUTER rejected provider-operation hint with status {status}: {body}"))]
    RouterRejected {
        status: reqwest::StatusCode,
        body: String,
    },

    #[snafu(display("Failed to build ROUTER URL from base {base_url}: {source}"))]
    RouterUrl {
        source: url::ParseError,
        base_url: String,
    },

    #[snafu(display("A discovery backend task terminated unexpectedly"))]
    DiscoveryTaskJoin { source: tokio::task::JoinError },

    #[snafu(display(
        "Discovery backend {backend} indexed lookup for watch {watch_id} timed out after {timeout_secs}s"
    ))]
    IndexedLookupTimeout {
        backend: String,
        watch_id: String,
        timeout_secs: u64,
    },

    #[snafu(display(
        "Discovery backend pending submission backlog exceeded {max_pending} deposits"
    ))]
    DiscoveryPendingSubmissionsTooLarge { max_pending: usize },

    #[snafu(display("Failed to initialize observability: {message}"))]
    Observability { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
