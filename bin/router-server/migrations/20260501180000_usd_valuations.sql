ALTER TABLE public.market_order_quotes
    ADD COLUMN IF NOT EXISTS usd_valuation_json jsonb DEFAULT '{}'::jsonb NOT NULL;

ALTER TABLE public.order_execution_steps
    ADD COLUMN IF NOT EXISTS usd_valuation_json jsonb DEFAULT '{}'::jsonb NOT NULL;

DO $$
BEGIN
    ALTER TABLE public.market_order_quotes
        ADD CONSTRAINT market_order_quotes_usd_valuation_object
        CHECK (jsonb_typeof(usd_valuation_json) = 'object');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE public.order_execution_steps
        ADD CONSTRAINT order_execution_steps_usd_valuation_object
        CHECK (jsonb_typeof(usd_valuation_json) = 'object');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
