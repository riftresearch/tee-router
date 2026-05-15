use hl_shim_client::{HlOrderStatus, HlShimClient, HlTransferKind};
use router_core::models::{
    ProviderOperationHintKind, ProviderOperationType, SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE,
};
use router_server::api::ProviderOperationHintRequest;
use router_temporal::{HlTradeCanceledEvidence, HlTradeFilledEvidence};
use serde_json::Value;

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
                hl_trade_filled_evidence(user, oid, fill),
            )
        }
        HlOrderStatus::Canceled | HlOrderStatus::Rejected | HlOrderStatus::MarginCanceled => (
            ProviderOperationHintKind::HlTradeCanceled,
            hl_trade_canceled_evidence(user, &order),
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

fn hl_trade_filled_evidence(
    user: alloy::primitives::Address,
    oid: u64,
    fill: FillEvidence,
) -> Value {
    typed_evidence(HlTradeFilledEvidence {
        user: format!("{user:?}"),
        oid,
        tid: fill.tid,
        coin: fill.coin,
        side: fill.side,
        px: fill.px,
        sz: fill.sz,
        crossed: fill.crossed,
        hash: fill.hash,
        time_ms: fill.time_ms,
    })
}

fn hl_trade_canceled_evidence(
    user: alloy::primitives::Address,
    order: &hl_shim_client::HlOrderEvent,
) -> Value {
    typed_evidence(HlTradeCanceledEvidence {
        user: format!("{user:?}"),
        oid: order.oid,
        coin: order.coin.clone(),
        status: format!("{:?}", order.status),
        status_timestamp_ms: order.status_timestamp_ms,
        reason: "terminal order status from HL shim".to_string(),
    })
}

fn typed_evidence<T: serde::Serialize>(evidence: T) -> Value {
    serde_json::to_value(evidence).expect("typed provider-operation evidence serializes")
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
    use serde::de::DeserializeOwned;
    use serde_json::json;
    use std::collections::BTreeSet;
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

    #[test]
    fn hl_trade_filled_evidence_matches_router_typed_shape() {
        let evidence = hl_trade_filled_evidence(
            user(),
            42,
            FillEvidence {
                tid: 7,
                coin: "BTC".to_string(),
                side: "B".to_string(),
                px: "65000".to_string(),
                sz: "0.1".to_string(),
                crossed: true,
                hash: "0xfill".to_string(),
                time_ms: 1_778_522_898_534,
            },
        );

        assert_typed_evidence::<HlTradeFilledEvidence>(
            &evidence,
            &[
                "user", "oid", "tid", "coin", "side", "px", "sz", "crossed", "hash", "time_ms",
            ],
        );
    }

    #[test]
    fn hl_trade_canceled_evidence_matches_router_typed_shape() {
        let order: hl_shim_client::HlOrderEvent = serde_json::from_value(json!({
            "user": "0x1111111111111111111111111111111111111111",
            "oid": 42,
            "cloid": null,
            "coin": "BTC",
            "side": "B",
            "limit_px": "65000",
            "sz": "0.1",
            "orig_sz": "0.1",
            "status": "canceled",
            "status_timestamp_ms": 1_778_522_898_534i64,
            "observed_at_ms": 1_778_522_898_535i64
        }))
        .expect("HL order event");

        let evidence = hl_trade_canceled_evidence(user(), &order);

        assert_typed_evidence::<HlTradeCanceledEvidence>(
            &evidence,
            &[
                "user",
                "oid",
                "coin",
                "status",
                "status_timestamp_ms",
                "reason",
            ],
        );
    }

    fn user() -> alloy::primitives::Address {
        "0x1111111111111111111111111111111111111111"
            .parse()
            .expect("address")
    }

    fn assert_typed_evidence<T: DeserializeOwned>(value: &serde_json::Value, expected: &[&str]) {
        let actual = value
            .as_object()
            .expect("evidence should be an object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let expected = expected.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        serde_json::from_value::<T>(value.clone()).expect("typed evidence should deserialize");
    }
}
