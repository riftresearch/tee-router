DROP INDEX IF EXISTS public.idx_order_execution_steps_wait_deposit_funding_tx;

CREATE INDEX IF NOT EXISTS idx_order_execution_legs_order_sort
    ON public.order_execution_legs USING btree (order_id, leg_index ASC, created_at ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_order_execution_steps_order_sort
    ON public.order_execution_steps USING btree (order_id, step_index ASC, created_at ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_order_provider_addresses_step_recipient_latest
    ON public.order_provider_addresses USING btree (
        execution_step_id,
        updated_at DESC,
        created_at DESC,
        id DESC
    )
    WHERE role = ANY (
        ARRAY[
            'across_recipient'::text,
            'unit_deposit'::text,
            'hyperliquid_destination'::text
        ]
    );

CREATE INDEX IF NOT EXISTS idx_router_orders_worker_due_updated
    ON public.router_orders USING btree (updated_at ASC, id ASC)
    WHERE order_type = ANY (ARRAY['market_order'::text, 'limit_order'::text])
      AND status = ANY (
        ARRAY[
            'pending_funding'::text,
            'funded'::text,
            'executing'::text,
            'refund_required'::text,
            'refunding'::text
        ]
      );

CREATE INDEX IF NOT EXISTS idx_order_execution_steps_failed_order
    ON public.order_execution_steps USING btree (order_id)
    WHERE status = 'failed'::text;

CREATE INDEX IF NOT EXISTS idx_deposit_vault_funding_hints_pending_claim_order
    ON public.deposit_vault_funding_hints USING btree (created_at ASC, id ASC)
    WHERE status = 'pending'::text;

CREATE INDEX IF NOT EXISTS idx_deposit_vault_funding_hints_processing_reclaim_order
    ON public.deposit_vault_funding_hints USING btree (
        claimed_at ASC,
        created_at ASC,
        id ASC
    )
    WHERE status = 'processing'::text;

CREATE INDEX IF NOT EXISTS idx_order_provider_operation_hints_pending_claim_order
    ON public.order_provider_operation_hints USING btree (created_at ASC, id ASC)
    WHERE status = 'pending'::text;

CREATE INDEX IF NOT EXISTS idx_order_provider_operation_hints_processing_reclaim_order
    ON public.order_provider_operation_hints USING btree (
        claimed_at ASC,
        created_at ASC,
        id ASC
    )
    WHERE status = 'processing'::text;
