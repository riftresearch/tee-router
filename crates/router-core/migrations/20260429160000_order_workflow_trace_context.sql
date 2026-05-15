ALTER TABLE public.router_orders
    ADD COLUMN IF NOT EXISTS workflow_trace_id text,
    ADD COLUMN IF NOT EXISTS workflow_parent_span_id text;

UPDATE public.router_orders
SET
    workflow_trace_id = replace(id::text, '-'::text, ''::text),
    workflow_parent_span_id = substring(md5(id::text) from 1 for 16)
WHERE workflow_trace_id IS NULL
   OR workflow_parent_span_id IS NULL;

ALTER TABLE public.router_orders
    ALTER COLUMN workflow_trace_id SET NOT NULL,
    ALTER COLUMN workflow_parent_span_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'router_orders_workflow_trace_id_check'
          AND conrelid = 'public.router_orders'::regclass
    ) THEN
        ALTER TABLE public.router_orders
            ADD CONSTRAINT router_orders_workflow_trace_id_check
                CHECK (
                    workflow_trace_id ~ '^[0-9a-f]{32}$'
                    AND workflow_trace_id <> '00000000000000000000000000000000'
                );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'router_orders_workflow_parent_span_id_check'
          AND conrelid = 'public.router_orders'::regclass
    ) THEN
        ALTER TABLE public.router_orders
            ADD CONSTRAINT router_orders_workflow_parent_span_id_check
                CHECK (
                    workflow_parent_span_id ~ '^[0-9a-f]{16}$'
                    AND workflow_parent_span_id <> '0000000000000000'
                );
    END IF;
END $$;
