use crate::{
    db::Database,
    error::RouterServerResult,
    models::{ProviderExecutionPolicyState, ProviderPolicy, ProviderQuotePolicyState},
};
use chrono::Utc;
use std::{collections::HashMap, sync::Arc, time::Duration, time::Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ProviderPolicySnapshot {
    policies: HashMap<String, ProviderPolicy>,
}

impl ProviderPolicySnapshot {
    #[must_use]
    pub fn policy(&self, provider: &str) -> ProviderPolicy {
        self.policies
            .get(provider)
            .cloned()
            .unwrap_or_else(|| ProviderPolicy::enabled(provider))
    }
}

#[derive(Debug, Clone)]
struct ProviderPolicyCache {
    fetched_at: Instant,
    snapshot: ProviderPolicySnapshot,
}

#[derive(Clone)]
pub struct ProviderPolicyService {
    db: Database,
    cache_ttl: Duration,
    cache: Arc<RwLock<Option<ProviderPolicyCache>>>,
}

impl ProviderPolicyService {
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self {
            db,
            cache_ttl: Duration::from_secs(5),
            cache: Arc::new(RwLock::new(None)),
        }
    }

    #[must_use]
    pub fn with_cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }

    pub async fn snapshot(&self) -> RouterServerResult<ProviderPolicySnapshot> {
        {
            let cache = self.cache.read().await;
            if let Some(cache) = cache.as_ref() {
                if cache.fetched_at.elapsed() < self.cache_ttl {
                    return Ok(cache.snapshot.clone());
                }
            }
        }

        let policies = self.db.provider_policies().list().await?;
        let snapshot = ProviderPolicySnapshot {
            policies: policies
                .into_iter()
                .map(|policy| (policy.provider.clone(), policy))
                .collect(),
        };
        let mut cache = self.cache.write().await;
        *cache = Some(ProviderPolicyCache {
            fetched_at: Instant::now(),
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    pub async fn list(&self) -> RouterServerResult<Vec<ProviderPolicy>> {
        let snapshot = self.snapshot().await?;
        let mut policies = snapshot.policies.into_values().collect::<Vec<_>>();
        policies.sort_by(|left, right| left.provider.cmp(&right.provider));
        Ok(policies)
    }

    pub async fn upsert(
        &self,
        provider: impl Into<String>,
        quote_state: ProviderQuotePolicyState,
        execution_state: ProviderExecutionPolicyState,
        reason: impl Into<String>,
        updated_by: impl Into<String>,
    ) -> RouterServerResult<ProviderPolicy> {
        let policy = ProviderPolicy {
            provider: provider.into(),
            quote_state,
            execution_state,
            reason: reason.into(),
            updated_by: updated_by.into(),
            updated_at: Utc::now(),
        };
        let stored = self.db.provider_policies().upsert(&policy).await?;
        let mut cache = self.cache.write().await;
        *cache = None;
        Ok(stored)
    }
}
