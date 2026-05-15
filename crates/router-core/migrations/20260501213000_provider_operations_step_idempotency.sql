DROP INDEX IF EXISTS idx_order_provider_operations_provider_ref;
DROP INDEX IF EXISTS idx_order_provider_operations_step_id;

CREATE UNIQUE INDEX idx_order_provider_operations_step_id
    ON public.order_provider_operations USING btree (execution_step_id)
    WHERE (execution_step_id IS NOT NULL);

CREATE INDEX idx_order_provider_operations_provider_ref
    ON public.order_provider_operations USING btree (provider, provider_ref)
    WHERE (provider_ref IS NOT NULL);
