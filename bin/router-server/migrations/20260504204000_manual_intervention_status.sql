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
                'failed'::text,
                'expired'::text
            ]
        )
    );

ALTER TABLE public.deposit_vaults
    DROP CONSTRAINT IF EXISTS deposit_vault_status_check;

ALTER TABLE public.deposit_vaults
    ADD CONSTRAINT deposit_vault_status_check CHECK (
        status = ANY (
            ARRAY[
                'pending_funding'::text,
                'funded'::text,
                'executing'::text,
                'completed'::text,
                'refund_required'::text,
                'refunding'::text,
                'refunded'::text,
                'manual_intervention_required'::text,
                'refund_manual_intervention_required'::text
            ]
        )
    );

DROP INDEX IF EXISTS public.idx_router_orders_in_progress_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_type_in_progress_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_failed_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_type_failed_created_at_id_desc;
DROP INDEX IF EXISTS public.idx_router_orders_worker_status_updated;

CREATE INDEX idx_router_orders_in_progress_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status NOT IN (
        'completed'::text,
        'refunded'::text,
        'failed'::text,
        'expired'::text,
        'manual_intervention_required'::text,
        'refund_manual_intervention_required'::text
    );

CREATE INDEX idx_router_orders_type_in_progress_created_at_id_desc
    ON public.router_orders USING btree (order_type, created_at DESC, id DESC)
    WHERE status NOT IN (
        'completed'::text,
        'refunded'::text,
        'failed'::text,
        'expired'::text,
        'manual_intervention_required'::text,
        'refund_manual_intervention_required'::text
    );

CREATE INDEX idx_router_orders_failed_created_at_id_desc
    ON public.router_orders USING btree (created_at DESC, id DESC)
    WHERE status = ANY (
        ARRAY[
            'failed'::text,
            'expired'::text,
            'refund_required'::text,
            'refunding'::text,
            'refunded'::text,
            'manual_intervention_required'::text,
            'refund_manual_intervention_required'::text
        ]
    );

CREATE INDEX idx_router_orders_type_failed_created_at_id_desc
    ON public.router_orders USING btree (order_type, created_at DESC, id DESC)
    WHERE status = ANY (
        ARRAY[
            'failed'::text,
            'expired'::text,
            'refund_required'::text,
            'refunding'::text,
            'refunded'::text,
            'manual_intervention_required'::text,
            'refund_manual_intervention_required'::text
        ]
    );

CREATE INDEX idx_router_orders_worker_status_updated
    ON public.router_orders USING btree (status, updated_at ASC, id ASC)
    INCLUDE (order_type, funding_vault_id)
    WHERE status IN (
        'pending_funding',
        'funded',
        'executing',
        'refund_required',
        'refunding',
        'manual_intervention_required',
        'refund_manual_intervention_required'
    );
