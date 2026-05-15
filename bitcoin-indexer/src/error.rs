use bitcoin::Txid;
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("invalid configuration: {message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("invalid Bitcoin address {value:?}: {source}"))]
    InvalidAddress {
        value: String,
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("invalid cursor {value:?}: {reason}"))]
    InvalidCursor { value: String, reason: String },

    #[snafu(display("Bitcoin RPC client init failed: {source}"))]
    BitcoinRpcInit {
        source: bitcoincore_rpc_async::Error,
    },

    #[snafu(display("Bitcoin RPC request {method} failed: {source}"))]
    BitcoinRpc {
        method: &'static str,
        source: bitcoincore_rpc_async::Error,
    },

    #[snafu(display("Bitcoin Esplora request failed: {source}"))]
    Esplora { source: esplora_client::Error },

    #[snafu(display("Bitcoin transaction {txid} was not found in Esplora"))]
    MissingTransaction { txid: Txid },

    #[snafu(display("Bitcoin ZMQ {topic} stream failed: {source}"))]
    Zmq {
        topic: &'static str,
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("Bitcoin consensus decode failed for {context}: {source}"))]
    ConsensusDecode {
        context: &'static str,
        source: bitcoin::consensus::encode::Error,
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
