use crate::{
    api::{
        ManualInterventionOrderContext, ManualInterventionOrderEnvelope,
        ManualInterventionOrdersEnvelope, OrderFlow, OrderFlowEnvelope, OrderFlowProgress,
        OrderFlowTrace,
    },
    error::{RouterServerError, RouterServerResult},
};
use router_core::{
    db::Database,
    error::RouterCoreError,
    models::{OrderExecutionAttemptKind, RouterOrderStatus},
};
use router_temporal::{order_workflow_id, refund_workflow_id};
use uuid::Uuid;

pub async fn get_order_flow(db: &Database, id: Uuid) -> RouterServerResult<OrderFlowEnvelope> {
    let order = db.orders().get(id).await.map_err(RouterServerError::from)?;
    let quote = match db.orders().get_router_order_quote(id).await {
        Ok(quote) => Some(quote),
        Err(RouterCoreError::NotFound) => None,
        Err(err) => return Err(err.into()),
    };
    let attempts = db
        .orders()
        .get_execution_attempts(id)
        .await
        .map_err(RouterServerError::from)?;
    let legs = db
        .orders()
        .get_execution_legs(id)
        .await
        .map_err(RouterServerError::from)?;
    let steps = db
        .orders()
        .get_execution_steps(id)
        .await
        .map_err(RouterServerError::from)?;
    let provider_operations = db
        .orders()
        .get_provider_operations(id)
        .await
        .map_err(RouterServerError::from)?;
    let custody_vaults = db
        .orders()
        .get_custody_vaults(id)
        .await
        .map_err(RouterServerError::from)?;
    let progress = OrderFlowProgress::from_parts(&order, &steps, &provider_operations);
    let trace = OrderFlowTrace {
        trace_id: order.workflow_trace_id.clone(),
        parent_span_id: order.workflow_parent_span_id.clone(),
    };

    Ok(OrderFlowEnvelope {
        flow: OrderFlow {
            order,
            trace,
            progress,
            quote,
            attempts,
            legs,
            steps,
            provider_operations,
            custody_vaults,
        },
    })
}

pub async fn list_manual_intervention_orders(
    db: &Database,
    limit: i64,
) -> RouterServerResult<ManualInterventionOrdersEnvelope> {
    let orders = db
        .orders()
        .list_manual_intervention_orders(limit)
        .await
        .map_err(RouterServerError::from)?;
    let mut summaries = Vec::with_capacity(orders.len());
    for order in orders {
        let context = get_manual_intervention_order(db, order.id).await?;
        summaries.push(context.order.summary);
    }

    Ok(ManualInterventionOrdersEnvelope { orders: summaries })
}

pub async fn get_manual_intervention_order(
    db: &Database,
    id: Uuid,
) -> RouterServerResult<ManualInterventionOrderEnvelope> {
    let flow = get_order_flow(db, id).await?.flow;
    if !matches!(
        flow.order.status,
        RouterOrderStatus::ManualInterventionRequired
            | RouterOrderStatus::RefundManualInterventionRequired
    ) {
        return Err(RouterServerError::Validation {
            message: format!(
                "order {id} is {} instead of a manual-intervention state",
                flow.order.status.to_db_string()
            ),
        });
    }
    let (workflow_id, parent_workflow_id) = manual_intervention_workflow_ids(&flow);

    Ok(ManualInterventionOrderEnvelope {
        order: ManualInterventionOrderContext::from_flow(flow, workflow_id, parent_workflow_id),
    })
}

fn manual_intervention_workflow_ids(flow: &OrderFlow) -> (String, Option<String>) {
    let root_workflow_id = order_workflow_id(flow.order.id);
    let Some(attempt) = flow
        .attempts
        .iter()
        .max_by_key(|attempt| (attempt.attempt_index, attempt.updated_at))
    else {
        return (root_workflow_id, None);
    };
    if attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery {
        return (root_workflow_id, None);
    }
    let parent_attempt_id = attempt
        .failure_reason
        .get("failed_attempt_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse::<Uuid>().ok());
    let Some(parent_attempt_id) = parent_attempt_id else {
        return (root_workflow_id, None);
    };

    (
        refund_workflow_id(flow.order.id, parent_attempt_id),
        Some(root_workflow_id),
    )
}
