ALTER TABLE order_execution_steps
    DROP CONSTRAINT order_execution_steps_step_type_check;

ALTER TABLE order_execution_steps
    ADD CONSTRAINT order_execution_steps_step_type_check CHECK (
        step_type IN (
            'wait_for_deposit',
            'across_bridge',
            'unit_deposit',
            'unit_withdrawal',
            'hyperliquid_trade',
            'hyperliquid_bridge_deposit',
            'universal_router_swap',
            'refund'
        )
    );

ALTER TABLE order_provider_operations
    DROP CONSTRAINT order_provider_operations_type_check;

ALTER TABLE order_provider_operations
    ADD CONSTRAINT order_provider_operations_type_check CHECK (
        operation_type IN (
            'across_bridge',
            'unit_deposit',
            'unit_withdrawal',
            'hyperliquid_trade',
            'hyperliquid_bridge_deposit',
            'universal_router_swap'
        )
    );
