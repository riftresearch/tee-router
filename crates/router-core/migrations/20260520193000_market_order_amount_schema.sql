DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
            AND table_name = 'market_order_quotes'
            AND column_name = 'amount_out'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
            AND table_name = 'market_order_quotes'
            AND column_name = 'estimated_amount_out'
    ) THEN
        ALTER TABLE public.market_order_quotes
            RENAME COLUMN amount_out TO estimated_amount_out;
    END IF;
END
$$;

ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_amount_out_digits;
ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_min_amount_out_digits;
ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_max_amount_in_digits;
ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_order_kind_check;
ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_slippage_bps_check;
ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_positive_amounts;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.market_order_quotes'::regclass
            AND conname = 'market_order_quotes_estimated_amount_out_digits'
    ) THEN
        ALTER TABLE public.market_order_quotes
            ADD CONSTRAINT market_order_quotes_estimated_amount_out_digits
            CHECK (estimated_amount_out ~ '^[0-9]+$'::text);
    END IF;

    ALTER TABLE public.market_order_quotes
        ADD CONSTRAINT market_order_quotes_positive_amounts
        CHECK (
            amount_in ~ '^0*[1-9][0-9]*$'
            AND estimated_amount_out ~ '^0*[1-9][0-9]*$'
        );
END
$$;

ALTER TABLE public.market_order_quotes
    DROP COLUMN IF EXISTS order_kind,
    DROP COLUMN IF EXISTS min_amount_out,
    DROP COLUMN IF EXISTS max_amount_in,
    DROP COLUMN IF EXISTS slippage_bps;

UPDATE public.market_order_actions
SET amount_in = COALESCE(amount_in, amount_out, max_amount_in)
WHERE amount_in IS NULL;

ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_amount_out_digits;
ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_min_amount_out_digits;
ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_max_amount_in_digits;
ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_order_kind_check;
ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_shape_check;
ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_slippage_bps_check;
ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_positive_amounts;

ALTER TABLE public.market_order_actions
    ALTER COLUMN amount_in SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.market_order_actions'::regclass
            AND conname = 'market_order_actions_amount_in_digits'
    ) THEN
        ALTER TABLE public.market_order_actions
            ADD CONSTRAINT market_order_actions_amount_in_digits
            CHECK (amount_in ~ '^[0-9]+$'::text);
    END IF;

    ALTER TABLE public.market_order_actions
        ADD CONSTRAINT market_order_actions_positive_amounts
        CHECK (amount_in ~ '^0*[1-9][0-9]*$');
END
$$;

ALTER TABLE public.market_order_actions
    DROP COLUMN IF EXISTS order_kind,
    DROP COLUMN IF EXISTS min_amount_out,
    DROP COLUMN IF EXISTS amount_out,
    DROP COLUMN IF EXISTS max_amount_in,
    DROP COLUMN IF EXISTS slippage_bps;

ALTER TABLE public.order_execution_steps
    DROP CONSTRAINT IF EXISTS order_execution_steps_min_amount_out_digits;

ALTER TABLE public.order_execution_steps
    DROP COLUMN IF EXISTS min_amount_out;
