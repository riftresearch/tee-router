use hl_shim_client::{HlOrderStatus, HlShimClient, HlTransferKind};
use router_core::models::{
    ProviderOperationHintKind, ProviderOperationType, SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE,
};
use router_server::api::ProviderOperationHintRequest;
use serde_json::json;

use crate::provider_operations::ProviderOperationWatchEntry;

pub async fn trade_hint(
    client: &HlShimClient,
    operation: &ProviderOperationWatchEntry,
) -> crate::error::Result<Option<ProviderOperationHintRequest>> {
    if !matches!(
        operation.operation_type,
        ProviderOperationType::HyperliquidTrade | ProviderOperationType::HyperliquidLimitOrder
    ) {
        return Ok(None);
    }
    let Some(user) = operation
        .request
        .get("user")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse().ok())
    else {
        return Ok(None);
    };
    let Some(oid) = operation_oid(operation) else {
        return Ok(None);
    };
    client
        .watch_order(user, oid)
        .await
        .map_err(|source| crate::error::Error::HlShim { source })?;
    let page = client
        .orders(user, Some(0), Some(2_000), None)
        .await
        .map_err(|source| crate::error::Error::HlShim { source })?;
    let Some(order) = page.orders.into_iter().find(|order| order.oid == oid) else {
        return Ok(None);
    };
    let (kind, evidence) = match order.status {
        HlOrderStatus::Filled => {
            let Some(fill) = filled_transfer_for_oid(client, user, oid).await? else {
                return Ok(None);
            };
            (
                ProviderOperationHintKind::HlTradeFilled,
                json!({
                    "user": format!("{user:?}"),
                    "oid": oid,
                    "tid": fill.tid,
                    "coin": fill.coin,
                    "side": fill.side,
                    "px": fill.px,
                    "sz": fill.sz,
                    "crossed": fill.crossed,
                    "hash": fill.hash,
                    "time_ms": fill.time_ms,
                }),
            )
        }
        HlOrderStatus::Canceled | HlOrderStatus::Rejected | HlOrderStatus::MarginCanceled => (
            ProviderOperationHintKind::HlTradeCanceled,
            json!({
                "user": format!("{user:?}"),
                "oid": order.oid,
                "coin": order.coin,
                "status": format!("{:?}", order.status),
                "status_timestamp_ms": order.status_timestamp_ms,
                "reason": "terminal order status from HL shim",
            }),
        ),
        _ => return Ok(None),
    };
    Ok(Some(ProviderOperationHintRequest {
        provider_operation_id: operation.operation_id,
        execution_step_id: operation.execution_step_id,
        source: SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE.to_string(),
        hint_kind: kind,
        evidence,
        idempotency_key: Some(format!(
            "hl:{}:{}:{oid}",
            operation.operation_id,
            kind.to_db_string()
        )),
    }))
}

struct FillEvidence {
    tid: u64,
    coin: String,
    side: String,
    px: String,
    sz: String,
    crossed: bool,
    hash: String,
    time_ms: i64,
}

async fn filled_transfer_for_oid(
    client: &HlShimClient,
    user: alloy::primitives::Address,
    oid: u64,
) -> crate::error::Result<Option<FillEvidence>> {
    let page = client
        .transfers(user, Some(0), Some(2_000), None)
        .await
        .map_err(|source| crate::error::Error::HlShim { source })?;
    Ok(page.events.into_iter().find_map(|event| {
        let HlTransferKind::Fill {
            oid: event_oid,
            tid,
            side,
            px,
            sz,
            crossed,
            ..
        } = event.kind
        else {
            return None;
        };
        (event_oid == oid).then_some(FillEvidence {
            tid,
            coin: event.asset,
            side,
            px,
            sz,
            crossed,
            hash: event.hash,
            time_ms: event.time_ms,
        })
    }))
}

pub fn operation_oid(operation: &ProviderOperationWatchEntry) -> Option<u64> {
    [
        "/oid",
        "/provider_observed_state/order/order/oid",
        "/previous_observed_state/oid",
        "/previous_observed_state/provider_observed_state/order/order/oid",
    ]
    .iter()
    .find_map(|pointer| {
        operation
            .observed_state
            .pointer(pointer)
            .and_then(serde_json::Value::as_u64)
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use router_core::models::{ProviderOperationStatus, ProviderOperationType};
    use router_temporal::WorkflowStepId;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn operation_oid_reads_resting_observed_state() {
        let operation = ProviderOperationWatchEntry {
            operation_id: Uuid::now_v7(),
            execution_step_id: WorkflowStepId::from(Uuid::now_v7()),
            provider: "hyperliquid".to_string(),
            operation_type: ProviderOperationType::HyperliquidTrade,
            provider_ref: Some("0xtx".to_string()),
            status: ProviderOperationStatus::WaitingExternal,
            request: json!({}),
            response: json!({}),
            observed_state: json!({ "kind": "resting", "oid": 42 }),
            execution_step_request: json!({}),
            updated_at: Utc::now(),
        };

        assert_eq!(operation_oid(&operation), Some(42));
    }
}
