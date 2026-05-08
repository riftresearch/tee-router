ALTER TABLE public.market_order_quotes
    ALTER COLUMN slippage_bps DROP NOT NULL;

ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_slippage_bps_check;

ALTER TABLE public.market_order_quotes
    ADD CONSTRAINT market_order_quotes_slippage_bps_check
    CHECK (slippage_bps IS NULL OR (slippage_bps >= 0 AND slippage_bps <= 10000));

ALTER TABLE public.market_order_actions
    ALTER COLUMN slippage_bps DROP NOT NULL;

ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_slippage_bps_check;

ALTER TABLE public.market_order_actions
    ADD CONSTRAINT market_order_actions_slippage_bps_check
    CHECK (slippage_bps IS NULL OR (slippage_bps >= 0 AND slippage_bps <= 10000));

ALTER TABLE public.market_order_actions
    DROP CONSTRAINT IF EXISTS market_order_actions_shape_check;

ALTER TABLE public.market_order_actions
    ADD CONSTRAINT market_order_actions_shape_check
    CHECK (
        (
            order_kind = 'exact_in'
            AND amount_in IS NOT NULL
            AND amount_out IS NULL
            AND max_amount_in IS NULL
        )
        OR (
            order_kind = 'exact_out'
            AND amount_in IS NULL
            AND min_amount_out IS NULL
            AND amount_out IS NOT NULL
        )
    );

ALTER TABLE public.order_execution_attempts
    ADD COLUMN superseded_by_attempt_id uuid,
    ADD COLUMN superseded_reason_json jsonb DEFAULT '{}'::jsonb NOT NULL;

ALTER TABLE ONLY public.order_execution_attempts
    ADD CONSTRAINT order_execution_attempts_superseded_by_attempt_id_fkey
    FOREIGN KEY (superseded_by_attempt_id) REFERENCES public.order_execution_attempts(id) ON DELETE SET NULL;

ALTER TABLE public.order_execution_attempts
    ADD CONSTRAINT order_execution_attempts_superseded_reason_object
    CHECK (jsonb_typeof(superseded_reason_json) = 'object'::text);

ALTER TABLE public.order_execution_attempts
    DROP CONSTRAINT IF EXISTS order_execution_attempts_attempt_kind_check;

ALTER TABLE public.order_execution_attempts
    ADD CONSTRAINT order_execution_attempts_attempt_kind_check
    CHECK (attempt_kind = ANY (ARRAY[
        'primary_execution'::text,
        'retry_execution'::text,
        'refreshed_execution'::text,
        'refund_recovery'::text
    ]));

ALTER TABLE public.order_execution_attempts
    DROP CONSTRAINT IF EXISTS order_execution_attempts_status_check;

ALTER TABLE public.order_execution_attempts
    ADD CONSTRAINT order_execution_attempts_status_check
    CHECK (status = ANY (ARRAY[
        'planning'::text,
        'active'::text,
        'completed'::text,
        'failed'::text,
        'refund_required'::text,
        'manual_intervention_required'::text,
        'superseded'::text
    ]));

ALTER TABLE public.order_execution_steps
    DROP CONSTRAINT IF EXISTS order_execution_steps_status_check;

ALTER TABLE public.order_execution_steps
    ADD CONSTRAINT order_execution_steps_status_check
    CHECK (status = ANY (ARRAY[
        'planned'::text,
        'waiting'::text,
        'ready'::text,
        'running'::text,
        'completed'::text,
        'failed'::text,
        'skipped'::text,
        'cancelled'::text,
        'superseded'::text
    ]));

ALTER TABLE public.order_execution_steps
    DROP CONSTRAINT IF EXISTS order_execution_steps_terminal_completed_at;

ALTER TABLE public.order_execution_steps
    ADD CONSTRAINT order_execution_steps_terminal_completed_at
    CHECK (
        (
            status IN (
                'completed'::text,
                'failed'::text,
                'skipped'::text,
                'cancelled'::text,
                'superseded'::text
            )
            AND completed_at IS NOT NULL
        )
        OR (
            status NOT IN (
                'completed'::text,
                'failed'::text,
                'skipped'::text,
                'cancelled'::text,
                'superseded'::text
            )
            AND completed_at IS NULL
        )
    );

ALTER TABLE public.order_execution_legs
    DROP CONSTRAINT IF EXISTS order_execution_legs_status_check;

ALTER TABLE public.order_execution_legs
    ADD CONSTRAINT order_execution_legs_status_check
    CHECK (status = ANY (ARRAY[
        'planned'::text,
        'waiting'::text,
        'ready'::text,
        'running'::text,
        'completed'::text,
        'failed'::text,
        'skipped'::text,
        'cancelled'::text,
        'superseded'::text
    ]));
