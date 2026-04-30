use crate::{
    api::{OrderFlow, OrderFlowEnvelope, OrderFlowProgress, OrderFlowTrace},
    db::Database,
    error::{RouterServerError, RouterServerResult},
};
use uuid::Uuid;

pub async fn get_order_flow(db: &Database, id: Uuid) -> RouterServerResult<OrderFlowEnvelope> {
    let order = db.orders().get(id).await?;
    let quote = match db.orders().get_market_order_quote(id).await {
        Ok(quote) => Some(quote),
        Err(RouterServerError::NotFound) => None,
        Err(err) => return Err(err),
    };
    let attempts = db.orders().get_execution_attempts(id).await?;
    let steps = db.orders().get_execution_steps(id).await?;
    let provider_operations = db.orders().get_provider_operations(id).await?;
    let custody_vaults = db.orders().get_custody_vaults(id).await?;
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
            steps,
            provider_operations,
            custody_vaults,
        },
    })
}
