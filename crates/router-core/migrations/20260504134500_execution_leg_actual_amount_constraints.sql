DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.order_execution_legs'::regclass
            AND conname = 'order_execution_legs_actual_amount_pair'
    ) THEN
        ALTER TABLE public.order_execution_legs
            ADD CONSTRAINT order_execution_legs_actual_amount_pair
            CHECK (
                (actual_amount_in IS NULL) = (actual_amount_out IS NULL)
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.order_execution_legs'::regclass
            AND conname = 'order_execution_legs_completed_actual_amounts'
    ) THEN
        ALTER TABLE public.order_execution_legs
            ADD CONSTRAINT order_execution_legs_completed_actual_amounts
            CHECK (
                (status = 'completed') = (
                    actual_amount_in IS NOT NULL
                    AND actual_amount_out IS NOT NULL
                )
            );
    END IF;
END
$$;
