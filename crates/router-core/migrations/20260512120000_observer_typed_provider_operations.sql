ALTER TABLE public.order_provider_operations
    DROP CONSTRAINT IF EXISTS order_provider_operations_type_check;

ALTER TABLE public.order_provider_operations
    ADD CONSTRAINT order_provider_operations_type_check
        CHECK (
            operation_type = ANY (
                ARRAY[
                    'across_bridge'::text,
                    'cctp_bridge'::text,
                    'cctp_receive'::text,
                    'unit_deposit'::text,
                    'unit_withdrawal'::text,
                    'hyperliquid_trade'::text,
                    'hyperliquid_limit_order'::text,
                    'hyperliquid_bridge_deposit'::text,
                    'hyperliquid_bridge_withdrawal'::text,
                    'universal_router_swap'::text
                ]
            )
        );

ALTER TABLE public.order_provider_operation_hints
    DROP CONSTRAINT IF EXISTS order_provider_operation_hints_kind_check;

ALTER TABLE public.order_provider_operation_hints
    ADD CONSTRAINT order_provider_operation_hints_kind_check CHECK (
        hint_kind = ANY (ARRAY[
            'possible_progress'::text,
            'across_destination_filled'::text,
            'cctp_receive_observed'::text,
            'velora_swap_settled'::text,
            'hl_trade_filled'::text,
            'hl_trade_canceled'::text,
            'hl_bridge_deposit_observed'::text,
            'hl_bridge_deposit_credited'::text,
            'hl_withdrawal_acknowledged'::text,
            'hl_withdrawal_settled'::text
        ])
    );
