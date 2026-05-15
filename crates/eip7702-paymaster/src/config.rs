use tracing::warn;

pub const EIP7702_PAYMASTER_BATCH_DEFAULT_MAX_SIZE: usize = 64;
pub const EIP7702_PAYMASTER_BATCH_MAX_SIZE_ENV: &str = "ROUTER_PAYMASTER_BATCH_MAX_SIZE";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaymasterBatchConfig {
    pub max_size: usize,
}

impl Default for PaymasterBatchConfig {
    fn default() -> Self {
        Self {
            max_size: EIP7702_PAYMASTER_BATCH_DEFAULT_MAX_SIZE,
        }
    }
}

impl PaymasterBatchConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            max_size: parse_paymaster_batch_max_size_env(default.max_size),
        }
    }
}

fn parse_paymaster_batch_max_size_env(default: usize) -> usize {
    match std::env::var(EIP7702_PAYMASTER_BATCH_MAX_SIZE_ENV) {
        Ok(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed > 0 => parsed,
            Ok(_) | Err(_) => {
                warn!(
                    env_var = EIP7702_PAYMASTER_BATCH_MAX_SIZE_ENV,
                    value, default, "invalid EVM paymaster batch max size; using default"
                );
                default
            }
        },
        Err(_) => default,
    }
}
