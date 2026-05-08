ALTER TABLE public.order_execution_legs
    ADD COLUMN IF NOT EXISTS provider_quote_expires_at timestamp with time zone;
