ALTER TABLE public.order_execution_steps
    DROP CONSTRAINT IF EXISTS order_execution_steps_step_type_check;

ALTER TABLE public.order_execution_steps
    ADD CONSTRAINT order_execution_steps_step_type_check
        CHECK (
            step_type = ANY (
                ARRAY[
                    'wait_for_deposit'::text,
                    'across_bridge'::text,
                    'cctp_burn'::text,
                    'cctp_receive'::text,
                    'unit_deposit'::text,
                    'unit_withdrawal'::text,
                    'hyperliquid_trade'::text,
                    'hyperliquid_limit_order'::text,
                    'hyperliquid_clearinghouse_to_spot'::text,
                    'hyperliquid_bridge_deposit'::text,
                    'hyperliquid_bridge_withdrawal'::text,
                    'universal_router_swap'::text,
                    'refund'::text
                ]
            )
        );
