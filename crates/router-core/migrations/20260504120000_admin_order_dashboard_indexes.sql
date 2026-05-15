CREATE INDEX IF NOT EXISTS idx_router_orders_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_router_orders_type_created_at_id_desc
    ON public.router_orders USING btree (order_type, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_router_orders_completed_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status = 'completed'::text;

CREATE INDEX IF NOT EXISTS idx_router_orders_in_progress_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status NOT IN (
        'completed'::text,
        'refunded'::text,
        'failed'::text,
        'expired'::text,
        'refund_manual_intervention_required'::text
    );

CREATE INDEX IF NOT EXISTS idx_router_orders_failed_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status = ANY (
        ARRAY[
            'failed'::text,
            'expired'::text,
            'refund_required'::text,
            'refunding'::text,
            'refunded'::text,
            'refund_manual_intervention_required'::text
        ]
    );

CREATE INDEX IF NOT EXISTS idx_router_orders_refunded_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status = 'refunded'::text;

CREATE INDEX IF NOT EXISTS idx_router_orders_manual_refund_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status = 'refund_manual_intervention_required'::text;

CREATE INDEX IF NOT EXISTS idx_order_execution_steps_wait_deposit_funding_tx
    ON public.order_execution_steps USING btree (order_id)
    WHERE step_type = 'wait_for_deposit'::text
      AND tx_hash IS NOT NULL;
