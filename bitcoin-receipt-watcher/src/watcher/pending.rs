use std::{collections::HashMap, sync::Arc, time::SystemTime};

use bitcoin::Txid;
use tokio::sync::RwLock;

use crate::{Error, Result};

#[derive(Debug, Clone)]
pub struct RegisteredWatch {
    pub chain: String,
    pub txid: Txid,
    pub requesting_operation_id: String,
    pub registered_at_ms: u128,
}

#[derive(Debug)]
struct PendingState {
    watches: HashMap<Txid, RegisteredWatch>,
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
        txid: Txid,
        requesting_operation_id: String,
    ) -> Result<RegisteredWatch> {
        let mut state = self.inner.write().await;
        if state.watches.len() >= state.max_pending && !state.watches.contains_key(&txid) {
            return Err(Error::PendingCapacity {
                max_pending: state.max_pending,
            });
        }

        let watch = RegisteredWatch {
            chain: self.chain.clone(),
            txid,
            requesting_operation_id,
            registered_at_ms: now_ms(),
        };
        state.watches.insert(txid, watch.clone());
        Ok(watch)
    }

    pub async fn cancel(&self, txid: &Txid) -> bool {
        self.inner.write().await.watches.remove(txid).is_some()
    }

    pub async fn take(&self, txid: &Txid) -> Option<RegisteredWatch> {
        self.inner.write().await.watches.remove(txid)
    }

    pub async fn matches_in_txids(&self, txids: &[Txid]) -> Vec<RegisteredWatch> {
        let state = self.inner.read().await;
        txids
            .iter()
            .filter_map(|txid| state.watches.get(txid).cloned())
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

    fn txid(byte: u8) -> Txid {
        let hex = format!("{byte:02x}").repeat(32);
        hex.parse().unwrap()
    }

    #[tokio::test]
    async fn register_replaces_same_txid_without_capacity_growth() {
        let pending = PendingWatches::new("bitcoin", 1);

        pending
            .register(txid(1), "operation-a".to_string())
            .await
            .unwrap();
        let replaced = pending
            .register(txid(1), "operation-b".to_string())
            .await
            .unwrap();

        assert_eq!(replaced.requesting_operation_id, "operation-b");
        assert_eq!(pending.len().await, 1);
    }

    #[tokio::test]
    async fn register_rejects_over_capacity() {
        let pending = PendingWatches::new("bitcoin", 1);

        pending
            .register(txid(1), "operation-a".to_string())
            .await
            .unwrap();
        let error = pending
            .register(txid(2), "operation-b".to_string())
            .await
            .unwrap_err();

        assert!(matches!(error, Error::PendingCapacity { max_pending: 1 }));
    }

    #[tokio::test]
    async fn matches_intersects_by_block_txids() {
        let pending = PendingWatches::new("bitcoin", 10);
        pending
            .register(txid(2), "operation-b".to_string())
            .await
            .unwrap();
        pending
            .register(txid(4), "operation-d".to_string())
            .await
            .unwrap();

        let matches = pending.matches_in_txids(&[txid(1), txid(2), txid(3)]).await;

        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].txid, txid(2));
    }
}
