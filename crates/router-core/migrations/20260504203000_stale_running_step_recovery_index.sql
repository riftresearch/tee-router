CREATE INDEX IF NOT EXISTS idx_router_orders_worker_status_updated
    ON public.router_orders USING btree (status, updated_at ASC, id ASC)
    INCLUDE (order_type, funding_vault_id)
    WHERE status IN (
        'pending_funding',
        'funded',
        'executing',
        'refund_required',
        'refunding',
        'refund_manual_intervention_required'
    );

CREATE INDEX IF NOT EXISTS idx_order_execution_steps_running_updated
    ON public.order_execution_steps USING btree (updated_at ASC, created_at ASC, id ASC)
    INCLUDE (order_id, execution_attempt_id)
    WHERE status = 'running' AND step_index > 0;

CREATE INDEX IF NOT EXISTS idx_order_provider_operations_terminal_updated_step
    ON public.order_provider_operations USING btree (updated_at ASC, created_at ASC, id ASC)
    INCLUDE (execution_step_id)
    WHERE status IN ('completed', 'failed', 'expired')
      AND execution_step_id IS NOT NULL;
