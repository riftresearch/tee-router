ALTER TABLE public.order_execution_steps
    DROP CONSTRAINT IF EXISTS order_execution_steps_hint_source_check;

ALTER TABLE public.order_execution_steps
    DROP COLUMN IF EXISTS hint_source;
