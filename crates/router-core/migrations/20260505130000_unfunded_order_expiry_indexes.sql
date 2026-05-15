DROP INDEX IF EXISTS public.idx_deposit_vaults_timeout_refund_claimable;

CREATE INDEX IF NOT EXISTS idx_deposit_vaults_timeout_refund_claimable
    ON public.deposit_vaults USING btree (cancel_after ASC, id ASC)
    WHERE status = 'funded'::text;

CREATE INDEX IF NOT EXISTS idx_router_orders_unfunded_expiry
    ON public.router_orders USING btree (action_timeout_at ASC, id ASC)
    WHERE status = ANY (ARRAY['quoted'::text, 'pending_funding'::text]);
