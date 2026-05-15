DROP INDEX IF EXISTS public.idx_router_orders_idempotency_key;

CREATE INDEX IF NOT EXISTS idx_router_orders_idempotency_key
  ON public.router_orders USING btree (idempotency_key)
  WHERE idempotency_key IS NOT NULL;
