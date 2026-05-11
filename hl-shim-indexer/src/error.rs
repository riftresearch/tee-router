use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("invalid configuration: {message}"))]
    InvalidConfiguration { message: String },

    #[snafu(display("invalid address {value:?}"))]
    InvalidAddress { value: String },

    #[snafu(display("invalid decimal {value:?}: {reason}"))]
    InvalidDecimal { value: String, reason: &'static str },

    #[snafu(display("invalid cursor {value:?}: {source}"))]
    InvalidCursor {
        value: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("database query failed: {source}"))]
    DatabaseQuery { source: sqlx_core::Error },

    #[snafu(display("database migration failed: {source}"))]
    DatabaseMigration {
        source: sqlx_core::migrate::MigrateError,
    },

    #[snafu(display("hyperliquid request failed for {endpoint}: {source}"))]
    HyperliquidRequest {
        endpoint: &'static str,
        source: hyperliquid_client::Error,
    },

    #[snafu(display("json serialization failed for {context}: {source}"))]
    Serialization {
        context: &'static str,
        source: serde_json::Error,
    },

    #[snafu(display("http server failed: {source}"))]
    HttpServer { source: std::io::Error },
}

impl From<sqlx_core::Error> for Error {
    fn from(source: sqlx_core::Error) -> Self {
        Self::DatabaseQuery { source }
    }
}
