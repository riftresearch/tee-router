DO $$
BEGIN
    ALTER TABLE public.market_order_quotes
        DROP CONSTRAINT IF EXISTS market_order_quotes_positive_amounts;
    ALTER TABLE public.market_order_quotes
        ADD CONSTRAINT market_order_quotes_positive_amounts
        CHECK (
            amount_in ~ '^0*[1-9][0-9]*$'
            AND amount_out ~ '^0*[1-9][0-9]*$'
            AND (
                max_amount_in IS NULL
                OR max_amount_in ~ '^0*[1-9][0-9]*$'
            )
        );

    ALTER TABLE public.market_order_actions
        DROP CONSTRAINT IF EXISTS market_order_actions_positive_amounts;
    ALTER TABLE public.market_order_actions
        ADD CONSTRAINT market_order_actions_positive_amounts
        CHECK (
            (
                amount_in IS NULL
                OR amount_in ~ '^0*[1-9][0-9]*$'
            )
            AND (
                amount_out IS NULL
                OR amount_out ~ '^0*[1-9][0-9]*$'
            )
            AND (
                max_amount_in IS NULL
                OR max_amount_in ~ '^0*[1-9][0-9]*$'
            )
        );

    ALTER TABLE public.limit_order_quotes
        DROP CONSTRAINT IF EXISTS limit_order_quotes_positive_input;
    ALTER TABLE public.limit_order_quotes
        DROP CONSTRAINT IF EXISTS limit_order_quotes_positive_output;
    ALTER TABLE public.limit_order_quotes
        ADD CONSTRAINT limit_order_quotes_positive_input
        CHECK (input_amount ~ '^0*[1-9][0-9]*$');
    ALTER TABLE public.limit_order_quotes
        ADD CONSTRAINT limit_order_quotes_positive_output
        CHECK (output_amount ~ '^0*[1-9][0-9]*$');

    ALTER TABLE public.limit_order_actions
        DROP CONSTRAINT IF EXISTS limit_order_actions_positive_input;
    ALTER TABLE public.limit_order_actions
        DROP CONSTRAINT IF EXISTS limit_order_actions_positive_output;
    ALTER TABLE public.limit_order_actions
        ADD CONSTRAINT limit_order_actions_positive_input
        CHECK (input_amount ~ '^0*[1-9][0-9]*$');
    ALTER TABLE public.limit_order_actions
        ADD CONSTRAINT limit_order_actions_positive_output
        CHECK (output_amount ~ '^0*[1-9][0-9]*$');

    ALTER TABLE public.order_execution_legs
        DROP CONSTRAINT IF EXISTS order_execution_legs_positive_amounts;
    ALTER TABLE public.order_execution_legs
        ADD CONSTRAINT order_execution_legs_positive_amounts
        CHECK (
            amount_in ~ '^0*[1-9][0-9]*$'
            AND expected_amount_out ~ '^0*[1-9][0-9]*$'
            AND (
                actual_amount_in IS NULL
                OR actual_amount_in ~ '^0*[1-9][0-9]*$'
            )
            AND (
                actual_amount_out IS NULL
                OR actual_amount_out ~ '^0*[1-9][0-9]*$'
            )
        );

    ALTER TABLE public.order_execution_steps
        DROP CONSTRAINT IF EXISTS order_execution_steps_positive_amount_in;
    ALTER TABLE public.order_execution_steps
        ADD CONSTRAINT order_execution_steps_positive_amount_in
        CHECK (
            amount_in IS NULL
            OR amount_in ~ '^0*[1-9][0-9]*$'
        );
END
$$;
