UPDATE public.router_orders
SET
    status = 'manual_intervention_required',
    updated_at = NOW()
WHERE status = 'failed';

ALTER TABLE public.router_orders
    DROP CONSTRAINT IF EXISTS router_orders_status_check;

ALTER TABLE public.router_orders
    ADD CONSTRAINT router_orders_status_check CHECK (
        status = ANY (
            ARRAY[
                'quoted'::text,
                'pending_funding'::text,
                'funded'::text,
                'executing'::text,
                'completed'::text,
                'refund_required'::text,
                'refunding'::text,
                'refunded'::text,
                'manual_intervention_required'::text,
                'refund_manual_intervention_required'::text,
                'expired'::text
            ]
        )
    );

DROP INDEX IF EXISTS public.idx_router_orders_in_progress_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_type_in_progress_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_failed_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_type_failed_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_needs_attention_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_type_needs_attention_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_terminal_custody_updated;

CREATE INDEX idx_router_orders_in_progress_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status NOT IN (
        'completed'::text,
        'refunded'::text,
        'expired'::text,
        'manual_intervention_required'::text,
        'refund_manual_intervention_required'::text
    );

CREATE INDEX idx_router_orders_type_in_progress_created_at_id_desc
    ON public.router_orders USING btree (order_type, created_at DESC, id DESC)
    WHERE status NOT IN (
        'completed'::text,
        'refunded'::text,
        'expired'::text,
        'manual_intervention_required'::text,
        'refund_manual_intervention_required'::text
    );

CREATE INDEX idx_router_orders_needs_attention_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status = ANY (
        ARRAY[
            'expired'::text,
            'refund_required'::text,
            'refunding'::text,
            'manual_intervention_required'::text,
            'refund_manual_intervention_required'::text
        ]
    );

CREATE INDEX idx_router_orders_type_needs_attention_created_at_id_desc
    ON public.router_orders USING btree (order_type, created_at DESC, id DESC)
    WHERE status = ANY (
        ARRAY[
            'expired'::text,
            'refund_required'::text,
            'refunding'::text,
            'manual_intervention_required'::text,
            'refund_manual_intervention_required'::text
        ]
    );

CREATE INDEX idx_router_orders_terminal_custody_updated
    ON public.router_orders USING btree (updated_at ASC, id ASC)
    WHERE status = ANY (
        ARRAY[
            'completed'::text,
            'refund_required'::text,
            'refunding'::text,
            'refunded'::text,
            'manual_intervention_required'::text,
            'refund_manual_intervention_required'::text,
            'expired'::text
        ]
    );
