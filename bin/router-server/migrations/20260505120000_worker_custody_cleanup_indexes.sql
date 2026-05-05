CREATE INDEX IF NOT EXISTS idx_router_orders_terminal_custody_updated
    ON public.router_orders USING btree (updated_at ASC, id ASC)
    WHERE status = ANY (
        ARRAY[
            'completed'::text,
            'refund_required'::text,
            'refunding'::text,
            'refunded'::text,
            'manual_intervention_required'::text,
            'failed'::text,
            'expired'::text
        ]
    );

CREATE INDEX IF NOT EXISTS idx_custody_vaults_internal_active_order
    ON public.custody_vaults USING btree (order_id)
    WHERE visibility = 'internal'::text
      AND role <> 'source_deposit'::text
      AND status = ANY (ARRAY['planned'::text, 'active'::text]);

CREATE INDEX IF NOT EXISTS idx_custody_vaults_released_internal_sweep_due
    ON public.custody_vaults USING btree (updated_at ASC, created_at ASC, id ASC)
    WHERE visibility = 'internal'::text
      AND role <> 'source_deposit'::text
      AND status = 'released'::text
      AND COALESCE(metadata_json ->> 'release_sweep_terminal', 'false') <> 'true';
