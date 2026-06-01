use crate::error::{RouterServerError, RouterServerResult};
use arc_swap::ArcSwap;
use chrono::Utc;
use router_core::{
    db::Database,
    models::{RouterSwitch, RouterSwitchName},
};
use std::sync::Arc;

#[derive(Clone)]
struct RouterSwitchSnapshot {
    refund_only_mode: RouterSwitch,
}

impl RouterSwitchSnapshot {
    fn from_switches(switches: Vec<RouterSwitch>) -> Self {
        let mut refund_only_mode = RouterSwitch::disabled(RouterSwitchName::RefundOnlyMode);
        for switch in switches {
            match switch.name {
                RouterSwitchName::RefundOnlyMode => refund_only_mode = switch,
            }
        }
        Self { refund_only_mode }
    }
}

pub struct RouterSwitchService {
    db: Database,
    snapshot: ArcSwap<RouterSwitchSnapshot>,
}

impl RouterSwitchService {
    pub async fn load(db: Database) -> RouterServerResult<Self> {
        let snapshot = RouterSwitchSnapshot::from_switches(
            db.router_switches()
                .list()
                .await
                .map_err(RouterServerError::from)?,
        );
        Ok(Self {
            db,
            snapshot: ArcSwap::from_pointee(snapshot),
        })
    }

    #[must_use]
    pub fn refund_only_mode(&self) -> RouterSwitch {
        self.snapshot.load().refund_only_mode.clone()
    }

    #[must_use]
    pub fn refund_only_enabled(&self) -> bool {
        self.snapshot.load().refund_only_mode.enabled
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
        let mut snapshot = self.snapshot.load().as_ref().clone();
        snapshot.refund_only_mode = stored.clone();
        self.snapshot.store(Arc::new(snapshot));
        Ok(stored)
    }
}
