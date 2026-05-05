DROP INDEX IF EXISTS public.idx_order_provider_addresses_order_provider_role_chain_address;

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_provider_addresses_step_provider_role_chain_address
    ON public.order_provider_addresses (execution_step_id, provider, role, chain_id, address)
    WHERE execution_step_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_provider_addresses_order_provider_role_chain_address_no_step
    ON public.order_provider_addresses (order_id, provider, role, chain_id, address)
    WHERE execution_step_id IS NULL;
