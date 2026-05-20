DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
            AND table_name = 'order_execution_legs'
            AND column_name = 'expected_amount_out'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
            AND table_name = 'order_execution_legs'
            AND column_name = 'estimated_amount_out'
    ) THEN
        ALTER TABLE public.order_execution_legs
            RENAME COLUMN expected_amount_out TO estimated_amount_out;
    END IF;
END
$$;

ALTER TABLE public.order_execution_legs
    DROP CONSTRAINT IF EXISTS order_execution_legs_expected_amount_out_digits;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.order_execution_legs'::regclass
            AND conname = 'order_execution_legs_estimated_amount_out_digits'
    ) THEN
        ALTER TABLE public.order_execution_legs
            ADD CONSTRAINT order_execution_legs_estimated_amount_out_digits
            CHECK (estimated_amount_out ~ '^[0-9]+$'::text);
    END IF;
END
$$;

ALTER TABLE public.order_execution_legs
    DROP CONSTRAINT IF EXISTS order_execution_legs_min_amount_out_digits;

ALTER TABLE public.order_execution_legs
    DROP COLUMN IF EXISTS min_amount_out;
