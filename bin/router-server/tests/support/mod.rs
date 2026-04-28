use std::{error::Error, future::Future, time::Duration};

use alloy::{primitives::B256, providers::Provider};

pub type LiveTestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const RPC_RETRY_ATTEMPTS: usize = 6;
const RECEIPT_POLL_ATTEMPTS: usize = 120;
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);

pub fn box_error<E>(err: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(err)
}

pub async fn retry_rpc<T, Fut, Op>(label: &str, mut op: Op) -> LiveTestResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = LiveTestResult<T>>,
{
    let mut delay = Duration::from_millis(500);
    for attempt in 1..=RPC_RETRY_ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < RPC_RETRY_ATTEMPTS && is_retryable_rpc_error(err.as_ref()) => {
                eprintln!(
                    "retrying RPC {label} after attempt {attempt}/{RPC_RETRY_ATTEMPTS}: {err}"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("retry loop always returns before exhausting attempts")
}

pub async fn wait_for_successful_receipt<P>(
    provider: &P,
    tx_hash: B256,
    label: &str,
) -> LiveTestResult<()>
where
    P: Provider,
{
    for _ in 0..RECEIPT_POLL_ATTEMPTS {
        let receipt = retry_rpc(&format!("{label} receipt"), || async {
            provider
                .get_transaction_receipt(tx_hash)
                .await
                .map_err(box_error)
        })
        .await?;

        if let Some(receipt) = receipt {
            if !receipt.status() {
                return Err(format!("{label} transaction {tx_hash} reverted").into());
            }
            return Ok(());
        }

        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }

    Err(format!("{label} transaction {tx_hash} was not mined before timeout").into())
}

fn is_retryable_rpc_error(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(err) = current {
        let message = err.to_string().to_ascii_lowercase();
        if message.contains("429")
            || message.contains("rate limit")
            || message.contains("rate-limited")
            || message.contains("too many requests")
            || message.contains("timeout")
            || message.contains("timed out")
            || message.contains("temporarily unavailable")
            || message.contains("temporary internal error")
            || message.contains("unknown block")
            || message.contains("connection reset")
            || message.contains("connection closed")
            || message.contains("connection refused")
            || message.contains("502")
            || message.contains("503")
            || message.contains("504")
        {
            return true;
        }
        current = err.source();
    }
    false
}
