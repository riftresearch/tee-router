use crate::error::{RouterServerError, RouterServerResult};
use chrono::Utc;
use router_core::{
    db::Database,
    models::{RouterSwitch, RouterSwitchName},
};
use std::{sync::Arc, time::Duration, time::Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
struct RouterSwitchCache {
    fetched_at: Instant,
    refund_only_mode: RouterSwitch,
}

#[derive(Clone)]
pub struct RouterSwitchService {
    db: Database,
    cache_ttl: Duration,
    cache: Arc<RwLock<Option<RouterSwitchCache>>>,
}

impl RouterSwitchService {
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self {
            db,
            cache_ttl: Duration::from_secs(2),
            cache: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn refund_only_mode(&self) -> RouterServerResult<RouterSwitch> {
        {
            let cache = self.cache.read().await;
            if let Some(cache) = cache.as_ref() {
                if cache.fetched_at.elapsed() < self.cache_ttl {
                    return Ok(cache.refund_only_mode.clone());
                }
            }
        }

        let refund_only_mode = self
            .db
            .router_switches()
            .get_or_default(RouterSwitchName::RefundOnlyMode)
            .await
            .map_err(RouterServerError::from)?;
        let mut cache = self.cache.write().await;
        *cache = Some(RouterSwitchCache {
            fetched_at: Instant::now(),
            refund_only_mode: refund_only_mode.clone(),
        });
        Ok(refund_only_mode)
    }

    pub async fn refund_only_enabled(&self) -> RouterServerResult<bool> {
        Ok(self.refund_only_mode().await?.enabled)
    }

    pub async fn set_refund_only_mode(
        &self,
        enabled: bool,
        reason: impl Into<String>,
        updated_by: impl Into<String>,
    ) -> RouterServerResult<RouterSwitch> {
        let switch = RouterSwitch {
            name: RouterSwitchName::RefundOnlyMode,
            enabled,
            reason: reason.into(),
            updated_by: updated_by.into(),
            updated_at: Utc::now(),
        };
        let stored = self
            .db
            .router_switches()
            .upsert(&switch)
            .await
            .map_err(RouterServerError::from)?;
        let mut cache = self.cache.write().await;
        *cache = None;
        Ok(stored)
    }
}
