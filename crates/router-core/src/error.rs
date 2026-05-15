use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum RouterCoreError {
    #[snafu(display("Database query failed: {}", source))]
    DatabaseQuery { source: sqlx_core::Error },

    #[snafu(display("Record not found"))]
    NotFound,

    #[snafu(display("Invalid data format: {}", message))]
    InvalidData { message: String },

    #[snafu(display("Database migration failed: {}", source))]
    Migration {
        source: sqlx_core::migrate::MigrateError,
    },

    #[snafu(display("Validation error: {}", message))]
    Validation { message: String },

    #[snafu(display("Conflict: {}", message))]
    Conflict { message: String },

    #[snafu(display("Unauthorized: {}", message))]
    Unauthorized { message: String },

    #[snafu(display("Forbidden: {}", message))]
    Forbidden { message: String },

    #[snafu(display("No route found: {}", message))]
    NoRoute { message: String },

    #[snafu(display("Not ready: {}", message))]
    NotReady { message: String },

    #[snafu(display("Internal server error: {}", message))]
    Internal { message: String },
}

impl From<sqlx_core::Error> for RouterCoreError {
    fn from(err: sqlx_core::Error) -> Self {
        match err {
            sqlx_core::Error::RowNotFound => Self::NotFound,
            _ => Self::DatabaseQuery { source: err },
        }
    }
}

impl From<sqlx_core::migrate::MigrateError> for RouterCoreError {
    fn from(err: sqlx_core::migrate::MigrateError) -> Self {
        Self::Migration { source: err }
    }
}

impl From<chains::Error> for RouterCoreError {
    fn from(err: chains::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}

pub type RouterCoreResult<T> = Result<T, RouterCoreError>;
