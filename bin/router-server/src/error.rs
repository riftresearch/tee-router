use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum RouterServerError {
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

impl From<sqlx_core::Error> for RouterServerError {
    fn from(err: sqlx_core::Error) -> Self {
        match err {
            sqlx_core::Error::RowNotFound => Self::NotFound,
            _ => Self::DatabaseQuery { source: err },
        }
    }
}

impl From<sqlx_core::migrate::MigrateError> for RouterServerError {
    fn from(err: sqlx_core::migrate::MigrateError) -> Self {
        Self::Migration { source: err }
    }
}

impl IntoResponse for RouterServerError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        if status.is_server_error() {
            tracing::error!(error = %self, "router API request failed");
        }

        let body = Json(json!({
            "error": {
                "code": status.as_u16(),
                "message": self.public_message(),
            }
        }));

        (status, body).into_response()
    }
}

impl RouterServerError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unauthorized { .. } => StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => StatusCode::FORBIDDEN,
            Self::Validation { .. } => StatusCode::BAD_REQUEST,
            Self::Conflict { .. } => StatusCode::CONFLICT,
            Self::NoRoute { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            Self::NotReady { .. } => StatusCode::TOO_EARLY,
            Self::InvalidData { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::DatabaseQuery { .. } | Self::Migration { .. } | Self::Internal { .. } => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    fn public_message(&self) -> String {
        match self {
            Self::DatabaseQuery { .. }
            | Self::InvalidData { .. }
            | Self::Migration { .. }
            | Self::Internal { .. } => "Internal server error".to_string(),
            Self::Unauthorized { .. } => "Unauthorized".to_string(),
            Self::Forbidden { .. } => "Forbidden".to_string(),
            Self::NotFound
            | Self::Validation { .. }
            | Self::Conflict { .. }
            | Self::NoRoute { .. }
            | Self::NotReady { .. } => self.to_string(),
        }
    }
}

impl From<crate::services::address_screening::AddressScreeningError> for RouterServerError {
    fn from(err: crate::services::address_screening::AddressScreeningError) -> Self {
        match err {
            crate::services::address_screening::AddressScreeningError::Blocked { .. } => {
                Self::Forbidden {
                    message: err.to_string(),
                }
            }
            crate::services::address_screening::AddressScreeningError::Request { .. } => {
                Self::Internal {
                    message: err.to_string(),
                }
            }
        }
    }
}

pub type RouterServerResult<T> = Result<T, RouterServerError>;

impl From<chains::Error> for RouterServerError {
    fn from(err: chains::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}

impl From<crate::services::vault_manager::VaultError> for RouterServerError {
    fn from(err: crate::services::vault_manager::VaultError) -> Self {
        use crate::services::vault_manager::VaultError;

        match err {
            VaultError::ChainNotSupported { chain } => Self::Validation {
                message: format!("Chain not supported: {chain}"),
            },
            VaultError::InvalidAssetId {
                asset,
                chain,
                reason,
            } => Self::Validation {
                message: format!("Invalid asset id {asset} for {chain}: {reason}"),
            },
            VaultError::InvalidRecoveryAddress { address, chain } => Self::Validation {
                message: format!("Invalid recovery address {address} for {chain}"),
            },
            VaultError::InvalidMetadata { reason } => Self::Validation { message: reason },
            VaultError::InvalidCancellationCommitment { reason } => {
                Self::Validation { message: reason }
            }
            VaultError::InvalidCancellationSecret => Self::Validation {
                message: "Invalid cancellation secret".to_string(),
            },
            VaultError::RefundNotAllowed { reason } => Self::Validation { message: reason },
            VaultError::InvalidOrderBinding { reason } => Self::Validation { message: reason },
            VaultError::InvalidFundingAmount { field, reason } => Self::Validation {
                message: format!("Invalid funding amount {field}: {reason}"),
            },
            VaultError::InvalidFundingHint { reason } => Self::Validation { message: reason },
            VaultError::FundingHintNotReady { reason } => Self::NotReady { message: reason },
            VaultError::FundingCheck { message } => Self::Internal { message },
            VaultError::Random { source } => Self::Internal {
                message: source.to_string(),
            },
            VaultError::WalletDerivation { source } => Self::Internal {
                message: source.to_string(),
            },
            VaultError::DepositAddress { source } => Self::Internal {
                message: source.to_string(),
            },
            VaultError::Database { source } => Self::Internal {
                message: source.to_string(),
            },
        }
    }
}

impl From<crate::services::order_manager::MarketOrderError> for RouterServerError {
    fn from(err: crate::services::order_manager::MarketOrderError) -> Self {
        use crate::services::order_manager::MarketOrderError;

        match err {
            MarketOrderError::ChainNotSupported { chain } => Self::Validation {
                message: format!("Chain not supported: {chain}"),
            },
            MarketOrderError::InvalidAssetId {
                asset,
                chain,
                reason,
            } => Self::Validation {
                message: format!("Invalid asset id {asset} for {chain}: {reason}"),
            },
            MarketOrderError::InvalidRecipientAddress { address, chain } => Self::Validation {
                message: format!("Invalid recipient address {address} for {chain}"),
            },
            MarketOrderError::InvalidRefundAddress { address, chain } => Self::Validation {
                message: format!("Invalid refund address {address} for {chain}"),
            },
            MarketOrderError::InvalidAmount { field, reason } => Self::Validation {
                message: format!("Invalid amount {field}: {reason}"),
            },
            MarketOrderError::InvalidIdempotencyKey { reason } => {
                Self::Validation { message: reason }
            }
            MarketOrderError::NoRoute { reason } => Self::NoRoute { message: reason },
            MarketOrderError::InputBelowRouteMinimum {
                amount_in,
                operational_min_input,
                hard_min_input,
                source_chain,
                source_asset,
                destination_chain,
                destination_asset,
            } => Self::Validation {
                message: format!(
                    "Input amount {amount_in} is below operational route minimum {operational_min_input} (hard minimum {hard_min_input}) for {source_chain} {source_asset} to {destination_chain} {destination_asset}"
                ),
            },
            MarketOrderError::RouteMinimum { reason } => Self::Internal { message: reason },
            MarketOrderError::GasReimbursement { source } => Self::Internal {
                message: source.to_string(),
            },
            MarketOrderError::QuoteExpired => Self::Validation {
                message: "Quote is expired".to_string(),
            },
            MarketOrderError::QuoteAlreadyOrdered => Self::Validation {
                message: "Quote has already been used to create an order".to_string(),
            },
            MarketOrderError::DepositAddress { source } => Self::Internal {
                message: source.to_string(),
            },
            MarketOrderError::Database { source } => Self::Internal {
                message: source.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn server_errors_do_not_expose_internal_details() {
        let response = RouterServerError::DatabaseQuery {
            source: sqlx_core::Error::Protocol(
                "private relation router_orders_private failed".to_string(),
            ),
        }
        .into_response();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"]["message"], "Internal server error");
        assert!(!body.to_string().contains("router_orders_private"));
    }

    #[test]
    fn validation_errors_remain_actionable() {
        let error = RouterServerError::Validation {
            message: "bad input".to_string(),
        };

        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.public_message(), "Validation error: bad input");
    }

    #[test]
    fn not_ready_is_retryable_without_being_a_server_error() {
        let error = RouterServerError::NotReady {
            message: "backend observation is still catching up".to_string(),
        };

        assert_eq!(error.status_code(), StatusCode::TOO_EARLY);
        assert!(!error.status_code().is_server_error());
        assert_eq!(
            error.public_message(),
            "Not ready: backend observation is still catching up"
        );
    }

    #[test]
    fn auth_errors_do_not_expose_configuration_state() {
        let error = RouterServerError::Unauthorized {
            message: "detector API key is not configured".to_string(),
        };

        assert_eq!(error.status_code(), StatusCode::UNAUTHORIZED);
        assert_eq!(error.public_message(), "Unauthorized");
    }
}
