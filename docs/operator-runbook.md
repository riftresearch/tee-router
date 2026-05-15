# Operator Runbook

This runbook covers day-to-day order execution operations after the Temporal
cutover. Postgres remains the canonical business-state store. Temporal owns
workflow history and replay.

## When an Order Looks Stuck

1. Check the router dashboard.
   - `In-Progress Order Queue` shows orders still moving through funding,
     execution, refund-required, or refunding states.
   - `Refund Required Orders` shows orders that need operator attention.
   - `Refund Required Log Events` shows recent workflow log events that moved an
     order to the refund queue.
2. Read the order flow:
   `GET /internal/v1/orders/{order_id}/flow`
3. Open Temporal Web UI and search for the returned `workflow_id`.
   - Root execution workflows use `order:{order_id}:execution`.
   - Refund child workflows use `order:{order_id}:refund:{parent_attempt_id}`.
4. Compare Postgres state with Temporal history.
   - The order flow includes attempts, legs, steps, provider operations, and
     custody vaults.
   - Confirm no provider operation is still making durable progress before
     taking any out-of-band action.

## Common States

- `Quoted`: quote exists but no order has been created.
- `PendingFunding`: order exists and is waiting for deposit funding.
- `Funded`: deposit funding was observed and execution can start.
- `Executing`: the primary execution workflow is actively moving funds.
- `Completed`: execution finished successfully.
- `RefundRequired`: the system could not continue execution automatically and an
  admin should review the refund path.
- `Refunding`: the refund workflow is actively moving funds back.
- `Refunded`: refund recovery finished.

## Refund Review

`RefundRequired` is the admin queue. Before taking action, confirm all of the
following:

- The order status in Postgres is still `refund_required`.
- Temporal history has reached the same refund-required decision.
- There is no active provider operation that can still complete the intended
  route.
- Exactly one recoverable position exists before attempting recovery outside the
  workflow.

When the refund path is clear, start or repair the refund workflow using the
operational tooling for the deployment. The public gateway maps both
`refund_required` and `refunding` to `refund_pending`.
