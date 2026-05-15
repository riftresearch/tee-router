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

    #[snafu(display("invalid transaction id {value:?}"))]
    InvalidTxid { value: String },

    #[snafu(display("missing block {height} from RPC response"))]
    MissingBlock { height: u64 },

    #[snafu(display("Bitcoin RPC client init failed: {source}"))]
    BitcoinRpcInit {
        source: bitcoincore_rpc_async::Error,
    },

    #[snafu(display("Bitcoin RPC request {method} failed: {source}"))]
    BitcoinRpc {
        method: &'static str,
        source: bitcoincore_rpc_async::Error,
    },

    #[snafu(display("Bitcoin ZMQ rawblock stream failed: {source}"))]
    Zmq {
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("Bitcoin consensus decode failed for {context}: {source}"))]
    ConsensusDecode {
        context: &'static str,
        source: bitcoin::consensus::encode::Error,
    },

    #[snafu(display("Bitcoin hex decode failed for {context}: {source}"))]
    HexDecode {
        context: &'static str,
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
