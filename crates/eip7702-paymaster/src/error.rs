use alloy::primitives::{TxHash, U256};
use alloy::transports::{RpcError, TransportErrorKind};
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
pub enum PaymasterError {
    #[snafu(display("EVMRPCError at {loc}: {source}"))]
    EVMRpcError {
        source: RpcError<TransportErrorKind>,
        #[snafu(implicit)]
        loc: Location,
    },

    #[snafu(display("Transaction reverted: {tx_hash}"))]
    TransactionReverted { tx_hash: String },

    #[snafu(display("Insufficient balance: required {required}, available {available}"))]
    InsufficientBalance { required: U256, available: U256 },

    #[snafu(display("Numeric overflow while calculating {context}"))]
    NumericOverflow { context: &'static str },

    #[snafu(display("Serialization error: {message}"))]
    Serialization { message: String },

    #[snafu(display("Failed to dump to address: {message}"))]
    DumpToAddress { message: String },

    #[snafu(display("{message}"))]
    Actor { message: String },
}

pub type Result<T> = std::result::Result<T, PaymasterError>;

pub(crate) fn clone_batch_error(error: &PaymasterError) -> PaymasterError {
    match error {
        PaymasterError::EVMRpcError { .. } => PaymasterError::Actor {
            message: error.to_string(),
        },
        PaymasterError::TransactionReverted { tx_hash } => PaymasterError::TransactionReverted {
            tx_hash: tx_hash.clone(),
        },
        PaymasterError::InsufficientBalance {
            required,
            available,
        } => PaymasterError::InsufficientBalance {
            required: *required,
            available: *available,
        },
        PaymasterError::NumericOverflow { context } => PaymasterError::NumericOverflow { context },
        PaymasterError::Serialization { message } => PaymasterError::Serialization {
            message: message.clone(),
        },
        PaymasterError::DumpToAddress { message } => PaymasterError::DumpToAddress {
            message: message.clone(),
        },
        PaymasterError::Actor { message } => PaymasterError::Actor {
            message: message.clone(),
        },
    }
}

pub(crate) fn tx_reverted(tx_hash: TxHash) -> PaymasterError {
    PaymasterError::TransactionReverted {
        tx_hash: tx_hash.to_string(),
    }
}
