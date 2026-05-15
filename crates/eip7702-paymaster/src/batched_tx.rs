use crate::actor::{PaymasterActor, PaymasterExecution};
use crate::error::{tx_reverted, PaymasterError, Result};
use crate::metrics::{
    record_paymaster_balance_gwei, record_paymaster_batch_sent, record_paymaster_funding_failed,
    record_paymaster_funding_sent, record_paymaster_gas_estimate,
};
use alloy::network::TransactionBuilder;
use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::sol_types::SolValue;
use eip7702_delegator_contract::{
    EIP7702Delegator::{EIP7702DelegatorInstance, Execution},
    ModeCode, EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS,
};
use metrics::counter;
use std::{
    future::Future,
    str::FromStr,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tracing::warn;

pub(crate) const EVM_CALL_ESTIMATE_GAS_CAP: u64 = 1_000_000;
const EVM_RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const EVM_RECEIPT_TIMEOUT: Duration = Duration::from_secs(300);
const EVM_RPC_RETRY_ATTEMPTS: usize = 6;
const EVM_RPC_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(500);

impl PaymasterActor {
    pub(crate) async fn send_batched_tx(&self, executions: &[Execution]) -> Result<TxHash> {
        let delegator = EIP7702DelegatorInstance::new(
            Address::from_str(EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS).map_err(|_| {
                PaymasterError::Serialization {
                    message: "Invalid EIP-7702 delegator address".to_string(),
                }
            })?,
            self.wallet_provider.clone(),
        );

        let mut tx = delegator
            .execute_1(
                ModeCode::Batch.as_fixed_bytes32(),
                executions.to_vec().abi_encode().into(),
            )
            .into_transaction_request()
            .with_from(self.sponsor_address);
        tx.set_to(self.sponsor_address);

        let gas_limit = estimate_batched_paymaster_gas(self, tx.clone(), executions).await?;
        record_paymaster_gas_estimate(self.metric_label, "eip7702_batch_top_up", gas_limit);
        let fee_estimate = retry_evm_rpc(self.metric_label, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;

        let sponsor_balance = retry_evm_rpc(self.metric_label, "get_balance", || async {
            self.provider.get_balance(self.sponsor_address).await
        })
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        record_paymaster_balance_gwei(self.metric_label, "sponsor", sponsor_balance);
        let batch_value = executions.iter().try_fold(U256::ZERO, |acc, execution| {
            checked_u256_add(acc, execution.value, "paymaster batch execution value")
        })?;
        let reserved_fee = checked_gas_fee(
            gas_limit,
            max_fee_per_gas,
            "paymaster batch gas reservation",
        )?;
        let required_balance = checked_u256_add(
            batch_value,
            reserved_fee,
            "paymaster batch sponsor required balance",
        )?;
        if sponsor_balance < required_balance {
            record_paymaster_funding_failed(self.metric_label, "insufficient_balance");
            return Err(PaymasterError::InsufficientBalance {
                required: required_balance,
                available: sponsor_balance,
            });
        }

        let nonce = retry_evm_rpc(
            self.metric_label,
            "get_transaction_count_pending",
            || async {
                self.provider
                    .get_transaction_count(self.sponsor_address)
                    .pending()
                    .await
            },
        )
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;

        tx = tx
            .with_nonce(nonce)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

        let pending_tx = self
            .wallet_provider
            .send_transaction(tx)
            .await
            .map_err(|source| PaymasterError::EVMRpcError {
                source,
                loc: snafu::location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        record_paymaster_batch_sent(self.metric_label, executions.len());
        wait_for_evm_receipt(&self.wallet_provider, self.metric_label, tx_hash).await?;
        Ok(tx_hash)
    }

    pub(crate) async fn send_legacy_top_up(&self, prepared: PaymasterExecution) -> Result<TxHash> {
        let tx_hash = self.send_legacy_execution(prepared.execution).await?;
        record_paymaster_funding_sent(self.metric_label, prepared.top_up_amount);
        Ok(tx_hash)
    }

    pub(crate) async fn send_legacy_execution(&self, execution: Execution) -> Result<TxHash> {
        let estimate_request = TransactionRequest::default()
            .with_from(self.sponsor_address)
            .with_to(execution.target)
            .with_value(execution.value)
            .with_input(execution.callData.clone());
        let gas_limit = retry_evm_rpc(self.metric_label, "estimate_gas", || async {
            self.provider.estimate_gas(estimate_request.clone()).await
        })
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        record_paymaster_gas_estimate(self.metric_label, "native_top_up", gas_limit);
        let fee_estimate = retry_evm_rpc(self.metric_label, "estimate_eip1559_fees", || async {
            self.provider.estimate_eip1559_fees().await
        })
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
        let reserved_fee = checked_gas_fee(
            gas_limit,
            max_fee_per_gas,
            "paymaster top-up gas reservation",
        )?;
        let required_balance = checked_u256_add(
            execution.value,
            reserved_fee,
            "paymaster sponsor required balance",
        )?;
        let sponsor_balance = retry_evm_rpc(self.metric_label, "get_balance", || async {
            self.provider.get_balance(self.sponsor_address).await
        })
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        record_paymaster_balance_gwei(self.metric_label, "sponsor", sponsor_balance);
        if sponsor_balance < required_balance {
            record_paymaster_funding_failed(self.metric_label, "insufficient_balance");
            return Err(PaymasterError::InsufficientBalance {
                required: required_balance,
                available: sponsor_balance,
            });
        }

        let nonce = retry_evm_rpc(
            self.metric_label,
            "get_transaction_count_pending",
            || async {
                self.provider
                    .get_transaction_count(self.sponsor_address)
                    .pending()
                    .await
            },
        )
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;

        let transaction_request = TransactionRequest::default()
            .with_from(self.sponsor_address)
            .with_to(execution.target)
            .with_value(execution.value)
            .with_input(execution.callData)
            .with_nonce(nonce)
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

        let pending_tx = self
            .wallet_provider
            .send_transaction(transaction_request)
            .await
            .map_err(|source| PaymasterError::EVMRpcError {
                source,
                loc: snafu::location!(),
            })?;
        let tx_hash = *pending_tx.tx_hash();
        wait_for_evm_receipt(&self.wallet_provider, self.metric_label, tx_hash).await?;
        Ok(tx_hash)
    }
}

async fn estimate_batched_paymaster_gas(
    actor: &PaymasterActor,
    batch_tx: TransactionRequest,
    executions: &[Execution],
) -> Result<u64> {
    match retry_evm_rpc(actor.metric_label, "estimate_gas", || async {
        actor.provider.estimate_gas(batch_tx.clone()).await
    })
    .await
    {
        Ok(gas_limit) => Ok(gas_limit),
        Err(batch_error) => {
            warn!(
                chain = actor.metric_label,
                execution_count = executions.len(),
                error = %batch_error,
                "batched paymaster gas estimate failed; falling back to per-execution sum"
            );
            estimate_batched_paymaster_gas_from_executions(actor, executions).await
        }
    }
}

async fn estimate_batched_paymaster_gas_from_executions(
    actor: &PaymasterActor,
    executions: &[Execution],
) -> Result<u64> {
    let mut total_gas = 0u64;
    for execution in executions {
        let estimate_request = TransactionRequest::default()
            .with_from(actor.sponsor_address)
            .with_to(execution.target)
            .with_value(execution.value)
            .with_input(execution.callData.clone());
        let gas_limit = retry_evm_rpc(actor.metric_label, "estimate_gas", || async {
            actor.provider.estimate_gas(estimate_request.clone()).await
        })
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        total_gas = total_gas
            .checked_add(gas_limit)
            .ok_or(PaymasterError::NumericOverflow {
                context: "paymaster batch fallback gas sum",
            })?;
    }

    total_gas
        .checked_mul(12)
        .map(|gas| gas.div_ceil(10))
        .ok_or(PaymasterError::NumericOverflow {
            context: "paymaster batch fallback gas headroom",
        })
}

pub(crate) async fn wait_for_evm_receipt<P>(
    provider: &P,
    metric_label: &'static str,
    tx_hash: TxHash,
) -> Result<TransactionReceipt>
where
    P: Provider,
{
    let started = Instant::now();
    loop {
        let receipt = retry_evm_rpc(metric_label, "get_transaction_receipt", || async {
            provider.get_transaction_receipt(tx_hash).await
        })
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
        if let Some(receipt) = receipt {
            ensure_evm_receipt_success(tx_hash, receipt.status())?;
            return Ok(receipt);
        }

        if started.elapsed() >= EVM_RECEIPT_TIMEOUT {
            return Err(PaymasterError::DumpToAddress {
                message: format!("timed out waiting for EVM transaction receipt for {tx_hash}"),
            });
        }

        sleep(EVM_RECEIPT_POLL_INTERVAL).await;
    }
}

pub(crate) async fn retry_evm_rpc<T, E, Fut, Op>(
    metric_label: &'static str,
    rpc_method: &'static str,
    mut op: Op,
) -> std::result::Result<T, E>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = EVM_RPC_RETRY_INITIAL_DELAY;
    let mut attempt = 1;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < EVM_RPC_RETRY_ATTEMPTS && is_retryable_evm_rpc_error(&err) => {
                let error_kind = classify_evm_rpc_error(&err);
                record_evm_rpc_retry(metric_label, rpc_method, error_kind);
                warn!(
                    chain = metric_label,
                    rpc_method,
                    attempt,
                    max_attempts = EVM_RPC_RETRY_ATTEMPTS,
                    error_kind,
                    error = %err,
                    "retrying EVM RPC after retryable error"
                );
                sleep(delay).await;
                delay = delay.saturating_mul(2);
                attempt += 1;
            }
            Err(err) => {
                let error_kind = classify_evm_rpc_error(&err);
                record_evm_rpc_failure(metric_label, rpc_method, error_kind);
                warn!(
                    chain = metric_label,
                    rpc_method,
                    error_kind,
                    error = %err,
                    "EVM RPC failed"
                );
                return Err(err);
            }
        }
    }
}

pub(crate) async fn estimate_evm_gas_or_cap(
    provider: &DynProvider,
    metric_label: &'static str,
    request: TransactionRequest,
    operation: &'static str,
) -> Result<u64> {
    match retry_evm_rpc(metric_label, "estimate_gas", || async {
        provider.estimate_gas(request.clone()).await
    })
    .await
    {
        Ok(gas_limit) => Ok(gas_limit),
        Err(err) if is_balance_bound_estimate_error(&err) => {
            warn!(
                chain = metric_label,
                operation,
                gas_cap = EVM_CALL_ESTIMATE_GAS_CAP,
                error = %err,
                "falling back to capped EVM gas estimate after balance-bound simulation failure"
            );
            Ok(EVM_CALL_ESTIMATE_GAS_CAP)
        }
        Err(source) => Err(PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        }),
    }
}

fn is_balance_bound_estimate_error<E: std::fmt::Display>(err: &E) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("insufficient funds")
        || message.contains("insufficient balance")
        || message.contains("transfer amount exceeds balance")
}

fn is_retryable_evm_rpc_error<E: std::fmt::Display>(err: &E) -> bool {
    let error_kind = classify_evm_rpc_error(err);
    matches!(
        error_kind,
        "rate_limited" | "timeout" | "upstream_unavailable" | "connection" | "state_lag"
    )
}

fn classify_evm_rpc_error<E: std::fmt::Display>(err: &E) -> &'static str {
    let message = err.to_string().to_ascii_lowercase();
    if is_evm_execution_revert(&message) {
        "execution_reverted"
    } else if contains_status_code(&message, "429")
        || message.contains("rate limit")
        || message.contains("rate-limited")
        || message.contains("too many requests")
    {
        "rate_limited"
    } else if message.contains("timeout") || message.contains("timed out") {
        "timeout"
    } else if message.contains("temporarily unavailable")
        || message.contains("temporary internal error")
        || message.contains("incorrect response body")
        || message.contains("wrong json-rpc response")
        || contains_status_code(&message, "502")
        || contains_status_code(&message, "503")
        || contains_status_code(&message, "504")
        || contains_status_code(&message, "500")
    {
        "upstream_unavailable"
    } else if message.contains("connection reset")
        || message.contains("connection closed")
        || message.contains("connection refused")
    {
        "connection"
    } else if message.contains("insufficient funds") || message.contains("insufficient balance") {
        "state_lag"
    } else {
        "other"
    }
}

fn is_evm_execution_revert(message: &str) -> bool {
    message.contains("execution reverted")
        || message.contains("transaction reverted")
        || message.contains("revert reason")
        || message.contains("evm revert")
}

fn contains_status_code(message: &str, code: &str) -> bool {
    message.match_indices(code).any(|(idx, _)| {
        let before = message[..idx].chars().next_back();
        let after = message[idx + code.len()..].chars().next();
        before.is_none_or(|ch| !ch.is_ascii_alphanumeric())
            && after.is_none_or(|ch| !ch.is_ascii_alphanumeric())
    })
}

fn record_evm_rpc_retry(
    metric_label: &'static str,
    rpc_method: &'static str,
    error_kind: &'static str,
) {
    counter!(
        "tee_router_chain_rpc_retries_total",
        "rpc_method" => rpc_method,
        "chain" => metric_label,
        "error_kind" => error_kind,
    )
    .increment(1);
    counter!(
        "ethereum_rpc_retries_total",
        "method" => rpc_method,
        "chain" => metric_label,
        "error_kind" => error_kind,
    )
    .increment(1);
}

fn record_evm_rpc_failure(
    metric_label: &'static str,
    rpc_method: &'static str,
    error_kind: &'static str,
) {
    counter!(
        "tee_router_chain_rpc_errors_total",
        "rpc_method" => rpc_method,
        "status" => "retry_exhausted",
        "chain" => metric_label,
        "error_kind" => error_kind,
    )
    .increment(1);
    counter!(
        "ethereum_rpc_errors_total",
        "method" => rpc_method,
        "status" => "retry_exhausted",
        "chain" => metric_label,
        "error_kind" => error_kind,
    )
    .increment(1);
}

pub(crate) fn max_fee_per_gas_with_headroom(max_fee_per_gas: u128) -> Result<u128> {
    max_fee_per_gas
        .checked_mul(11)
        .map(|value| value.div_ceil(10))
        .ok_or(PaymasterError::NumericOverflow {
            context: "EVM max fee per gas headroom",
        })
}

pub(crate) fn checked_gas_fee(
    gas_limit: u64,
    max_fee_per_gas: u128,
    context: &'static str,
) -> Result<U256> {
    U256::from(gas_limit)
        .checked_mul(U256::from(max_fee_per_gas))
        .ok_or(PaymasterError::NumericOverflow { context })
}

pub(crate) fn checked_u256_add(left: U256, right: U256, context: &'static str) -> Result<U256> {
    left.checked_add(right)
        .ok_or(PaymasterError::NumericOverflow { context })
}

pub(crate) fn checked_u256_sub(left: U256, right: U256, context: &'static str) -> Result<U256> {
    left.checked_sub(right)
        .ok_or(PaymasterError::NumericOverflow { context })
}

fn ensure_evm_receipt_success(tx_hash: TxHash, succeeded: bool) -> Result<()> {
    if succeeded {
        Ok(())
    } else {
        Err(tx_reverted(tx_hash))
    }
}
