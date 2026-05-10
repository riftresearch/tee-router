# Operator Runbook

This runbook covers day-to-day order execution operations after the Temporal
cutover. Postgres remains the canonical business-state store. Temporal owns
workflow history and replay.

## When an Order Looks Stuck

1. Check the router dashboard.
   - `In-Progress Order Queue` shows orders still moving through funding,
     execution, refund-required, or refunding states.
   - `Manual Intervention Orders` and `Refund Manual Intervention Orders` show
     orders that need operator attention.
   - `Manual Intervention Log Events` shows recent workflow log events that
     moved an order to manual intervention.
2. Query the operator context.
   - List paused orders:
     `GET /internal/v1/orders/manual-interventions`
   - Limit the list:
     `GET /internal/v1/orders/manual-interventions?limit=25`
   - Read one paused order:
     `GET /internal/v1/orders/{order_id}/manual-intervention`
   - Use the configured admin bearer token. These endpoints are read-only in
     PR10-i.
3. Open Temporal Web UI and search for the returned `workflow_id`.
   - Root execution workflows use `order:{order_id}:execution`.
   - Refund child workflows use `order:{order_id}:refund:{parent_attempt_id}`.
   - If `parent_workflow_id` is present, open it as the root order workflow.
4. Compare Postgres state with Temporal history.
   - The endpoint returns the current attempt, last-known step, failure reason,
     current provider operation, and `last_activity_at`.
   - Use `/internal/v1/orders/{order_id}/flow` for the full attempt, step,
     provider-operation, and custody-vault trace.

## Common States

- `ManualInterventionRequired`: primary execution could not safely continue.
  Typical causes are stale-running-step ambiguity, provider failure classified
  as manual, or provider-hint timeout.
- `RefundManualInterventionRequired`: refund recovery could not safely continue.
  Typical causes are zero or multiple recoverable positions, refund execution
  failure, or a stale refund quote. RefundRecovery attempts intentionally do not
  stale-refresh because funds may be mid-flight.
- `RefundRequired`: the order has not completed, and the refund workflow should
  either be running or about to run. Check Temporal before intervening manually.

## Before Manual Action

Confirm all of the following:

- The order status in Postgres matches the operator context.
- The Temporal workflow history has reached the same failure or pause point.
- No provider operation is still making durable progress.
- For refunds, exactly one recoverable position exists before attempting any
  manual recovery outside the workflow.

## PR10-ii Placeholder

PR10-ii will convert manual intervention from a terminal workflow result into a
long-lived workflow pause. It will add operator actions for:

- releasing a paused order back into the workflow,
- triggering a refund from a paused primary execution,
- acknowledging an unrecoverable order.

Until PR10-ii lands, the PR10-i endpoints are read-only. Operators should use
them for diagnosis and coordinate any manual database or custody action through
the engineering runbook for the incident.
