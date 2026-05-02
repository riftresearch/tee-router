pub mod benchmarks;
pub mod cdc;
pub mod config;
pub mod cursor;
pub mod discovery;
pub mod error;
pub mod provider_operations;
pub mod router_client;
pub mod runtime;
pub mod state_db;
pub mod watch;

pub use config::SauronArgs;
pub use error::{Error, Result};
pub use runtime::run;
