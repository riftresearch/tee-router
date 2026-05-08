ALTER TABLE public.order_execution_steps
    DROP CONSTRAINT IF EXISTS order_execution_steps_execution_leg_id_fkey;

ALTER TABLE public.order_execution_steps
    ADD CONSTRAINT order_execution_steps_execution_leg_id_fkey
    FOREIGN KEY (execution_leg_id)
    REFERENCES public.order_execution_legs(id)
    ON DELETE CASCADE;

ALTER TABLE public.order_provider_operations
    DROP CONSTRAINT IF EXISTS order_provider_operations_execution_attempt_id_fkey;

ALTER TABLE public.order_provider_operations
    ADD CONSTRAINT order_provider_operations_execution_attempt_id_fkey
    FOREIGN KEY (execution_attempt_id)
    REFERENCES public.order_execution_attempts(id)
    ON DELETE CASCADE;

ALTER TABLE public.order_provider_operations
    DROP CONSTRAINT IF EXISTS order_provider_operations_execution_step_id_fkey;

ALTER TABLE public.order_provider_operations
    ADD CONSTRAINT order_provider_operations_execution_step_id_fkey
    FOREIGN KEY (execution_step_id)
    REFERENCES public.order_execution_steps(id)
    ON DELETE CASCADE;
