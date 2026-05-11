ALTER TABLE public.order_provider_operation_hints
    DROP CONSTRAINT IF EXISTS order_provider_operation_hints_kind_check;

ALTER TABLE public.order_provider_operation_hints
    ADD CONSTRAINT order_provider_operation_hints_kind_check CHECK (
        hint_kind = ANY (ARRAY[
            'possible_progress'::text,
            'hl_trade_filled'::text,
            'hl_trade_canceled'::text,
            'hl_bridge_deposit_observed'::text,
            'hl_bridge_deposit_credited'::text,
            'hl_withdrawal_acknowledged'::text,
            'hl_withdrawal_settled'::text
        ])
    );
