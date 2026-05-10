ALTER TABLE public.order_execution_steps
    ADD COLUMN IF NOT EXISTS waiting_external_at timestamp with time zone,
    ADD COLUMN IF NOT EXISTS hint_arrived_at timestamp with time zone,
    ADD COLUMN IF NOT EXISTS hint_source text;

ALTER TABLE public.order_execution_steps
    DROP CONSTRAINT IF EXISTS order_execution_steps_hint_source_check;

ALTER TABLE public.order_execution_steps
    ADD CONSTRAINT order_execution_steps_hint_source_check
    CHECK (
        hint_source IS NULL
        OR hint_source = ANY (ARRAY['sauron_signal'::text, 'hint_poll'::text])
    );
