use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("invalid configuration: {message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("watch request chain {actual:?} does not match watcher chain {expected:?}"))]
    ChainMismatch { expected: String, actual: String },

    #[snafu(display("pending watch set is full at {max_pending} entries"))]
    PendingCapacity { max_pending: usize },

    #[snafu(display("invalid transaction hash {value:?}"))]
    InvalidTxHash { value: String },

    #[snafu(display("missing block {height} from RPC response"))]
    MissingBlock { height: u64 },

    #[snafu(display("EVM RPC request {method} failed: {source}"))]
    EvmRpc {
        method: &'static str,
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("EVM websocket failed: {source}"))]
    WebSocket {
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("json serialization failed for {context}: {source}"))]
    Serialization {
        context: &'static str,
        source: serde_json::Error,
    },

    #[snafu(display("metrics setup failed: {message}"))]
    Metrics { message: String },

    #[snafu(display("http server failed: {source}"))]
    HttpServer { source: std::io::Error },
}
