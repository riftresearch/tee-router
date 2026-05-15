DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.order_execution_steps'::regclass
            AND conname = 'order_execution_steps_terminal_completed_at'
    ) THEN
        ALTER TABLE public.order_execution_steps
            ADD CONSTRAINT order_execution_steps_terminal_completed_at
            CHECK (
                (
                    status IN ('completed', 'failed', 'skipped', 'cancelled')
                    AND completed_at IS NOT NULL
                )
                OR (
                    status NOT IN ('completed', 'failed', 'skipped', 'cancelled')
                    AND completed_at IS NULL
                )
            );
    END IF;

    ALTER TABLE public.order_execution_steps
        DROP CONSTRAINT IF EXISTS order_execution_steps_funding_root_only;

    ALTER TABLE public.order_execution_steps
        ADD CONSTRAINT order_execution_steps_funding_root_only
        CHECK (
            (
                step_type = 'wait_for_deposit'
                AND execution_attempt_id IS NULL
                AND execution_leg_id IS NULL
            )
            OR (
                step_type <> 'wait_for_deposit'
                AND execution_leg_id IS NOT NULL
            )
        );

    ALTER TABLE public.order_execution_steps
        DROP CONSTRAINT IF EXISTS order_execution_steps_completed_external_ref;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.order_provider_operations'::regclass
            AND conname = 'order_provider_operations_step_required'
    ) THEN
        ALTER TABLE public.order_provider_operations
            ADD CONSTRAINT order_provider_operations_step_required
            CHECK (
                execution_attempt_id IS NOT NULL
                AND execution_step_id IS NOT NULL
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'public.order_provider_operations'::regclass
            AND conname = 'order_provider_operations_completed_ref'
    ) THEN
        ALTER TABLE public.order_provider_operations
            ADD CONSTRAINT order_provider_operations_completed_ref
            CHECK (
                status <> 'completed'
                OR provider_ref IS NOT NULL
            );
    END IF;
END
$$;
