ALTER TABLE public.limit_order_quotes
    ADD COLUMN IF NOT EXISTS usd_valuation_json jsonb DEFAULT '{}'::jsonb NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.limit_order_quotes'::regclass
            AND conname = 'limit_order_quotes_usd_valuation_object'
    ) THEN
        ALTER TABLE public.limit_order_quotes
            ADD CONSTRAINT limit_order_quotes_usd_valuation_object
            CHECK (jsonb_typeof(usd_valuation_json) = 'object');
    END IF;
END
$$;
