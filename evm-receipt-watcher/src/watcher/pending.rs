use std::{collections::HashMap, sync::Arc, time::SystemTime};

use alloy::primitives::TxHash;
use tokio::sync::RwLock;

use crate::{telemetry, Error, Result};

#[derive(Debug, Clone)]
pub struct RegisteredWatch {
    pub chain: String,
    pub tx_hash: TxHash,
    pub requesting_operation_id: String,
    pub registered_at_ms: u128,
}

#[derive(Debug)]
struct PendingState {
    watches: HashMap<TxHash, RegisteredWatch>,
    max_pending: usize,
}

#[derive(Debug, Clone)]
pub struct PendingWatches {
    chain: String,
    inner: Arc<RwLock<PendingState>>,
}

impl PendingWatches {
    pub fn new(chain: impl Into<String>, max_pending: usize) -> Self {
        Self {
            chain: chain.into(),
            inner: Arc::new(RwLock::new(PendingState {
                watches: HashMap::new(),
                max_pending,
            })),
        }
    }

    pub async fn register(
        &self,
        tx_hash: TxHash,
        requesting_operation_id: String,
    ) -> Result<RegisteredWatch> {
        let mut state = self.inner.write().await;
        if state.watches.len() >= state.max_pending && !state.watches.contains_key(&tx_hash) {
            telemetry::record_watch_registered(&self.chain, "rejected_capacity");
            return Err(Error::PendingCapacity {
                max_pending: state.max_pending,
            });
        }

        let watch = RegisteredWatch {
            chain: self.chain.clone(),
            tx_hash,
            requesting_operation_id,
            registered_at_ms: now_ms(),
        };
        let outcome = if state.watches.insert(tx_hash, watch.clone()).is_some() {
            "replaced"
        } else {
            "inserted"
        };
        telemetry::record_watch_registered(&self.chain, outcome);
        telemetry::record_pending_count(&self.chain, state.watches.len());
        Ok(watch)
    }

    pub async fn cancel(&self, tx_hash: &TxHash) -> bool {
        let mut state = self.inner.write().await;
        let found = state.watches.remove(tx_hash).is_some();
        telemetry::record_watch_cancelled(&self.chain, found);
        telemetry::record_pending_count(&self.chain, state.watches.len());
        found
    }

    pub async fn take(&self, tx_hash: &TxHash) -> Option<RegisteredWatch> {
        let mut state = self.inner.write().await;
        let watch = state.watches.remove(tx_hash);
        telemetry::record_pending_count(&self.chain, state.watches.len());
        watch
    }

    pub async fn matches_in_hashes(&self, tx_hashes: &[TxHash]) -> Vec<RegisteredWatch> {
        let state = self.inner.read().await;
        tx_hashes
            .iter()
            .filter_map(|tx_hash| state.watches.get(tx_hash).cloned())
            .collect()
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.watches.len()
    }

    pub async fn max_pending(&self) -> usize {
        self.inner.read().await.max_pending
    }
}

fn now_ms() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis(),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> TxHash {
        TxHash::repeat_byte(byte)
    }

    #[tokio::test]
    async fn register_replaces_same_hash_without_capacity_growth() {
        let pending = PendingWatches::new("base", 1);

        pending
            .register(hash(1), "operation-a".to_string())
            .await
            .unwrap();
        let replaced = pending
            .register(hash(1), "operation-b".to_string())
            .await
            .unwrap();

        assert_eq!(replaced.requesting_operation_id, "operation-b");
        assert_eq!(pending.len().await, 1);
    }

    #[tokio::test]
    async fn register_rejects_over_capacity() {
        let pending = PendingWatches::new("base", 1);

        pending
            .register(hash(1), "operation-a".to_string())
            .await
            .unwrap();
        let error = pending
            .register(hash(2), "operation-b".to_string())
            .await
            .unwrap_err();

        assert!(matches!(error, Error::PendingCapacity { max_pending: 1 }));
    }

    #[tokio::test]
    async fn matches_intersects_by_block_hashes_not_pending_scan() {
        let pending = PendingWatches::new("base", 10);
        pending
            .register(hash(2), "operation-b".to_string())
            .await
            .unwrap();
        pending
            .register(hash(4), "operation-d".to_string())
            .await
            .unwrap();

        let matches = pending
            .matches_in_hashes(&[hash(1), hash(2), hash(3)])
            .await;

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].tx_hash, hash(2));
    }
}
