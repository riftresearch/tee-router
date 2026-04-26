use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("invalid base URL: {source}"))]
    InvalidBaseUrl { source: url::ParseError },

    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    #[snafu(display("HTTP {status}: {body}"))]
    HttpStatus { status: u16, body: String },

    #[snafu(display("JSON (de)serialization failed: {source}"))]
    Json { source: serde_json::Error },

    #[snafu(display("msgpack serialization failed: {message}"))]
    Msgpack { message: String },

    #[snafu(display("failed to sign payload: {message}"))]
    Signature { message: String },

    #[snafu(display("asset {asset} not in coin-to-asset map"))]
    AssetNotFound { asset: String },

    #[snafu(display(
        "invalid Hyperliquid bridge deposit amount {amount}: minimum is {minimum} raw USDC units"
    ))]
    InvalidBridgeDepositAmount { amount: String, minimum: String },

    #[snafu(display("hyperliquid API error: {message}"))]
    Api { message: String },
}
