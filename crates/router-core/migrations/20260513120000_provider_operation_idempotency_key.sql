ALTER TABLE public.order_provider_operations
    ADD COLUMN IF NOT EXISTS idempotency_key text;

ALTER TABLE public.order_provider_operations
    DROP CONSTRAINT IF EXISTS order_provider_operations_idempotency_key_not_empty;

ALTER TABLE public.order_provider_operations
    ADD CONSTRAINT order_provider_operations_idempotency_key_not_empty
        CHECK (idempotency_key IS NULL OR idempotency_key <> '');

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_provider_operations_idempotency_key
    ON public.order_provider_operations (provider, operation_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
