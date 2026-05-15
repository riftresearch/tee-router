use crate::{
    api::{OrderFlow, OrderFlowEnvelope, OrderFlowProgress, OrderFlowTrace},
    error::{RouterServerError, RouterServerResult},
};
use router_core::{db::Database, error::RouterCoreError};
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
