ALTER TABLE public.router_orders
    DROP CONSTRAINT IF EXISTS router_orders_order_type_check;

ALTER TABLE public.router_orders
    ADD CONSTRAINT router_orders_order_type_check
        CHECK (order_type = ANY (ARRAY['market_order'::text, 'limit_order'::text]));

CREATE TABLE public.limit_order_actions (
    order_id uuid NOT NULL,
    input_amount text NOT NULL,
    output_amount text NOT NULL,
    residual_policy text DEFAULT 'refund'::text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT limit_order_actions_input_amount_digits CHECK (input_amount ~ '^[0-9]+$'::text),
    CONSTRAINT limit_order_actions_output_amount_digits CHECK (output_amount ~ '^[0-9]+$'::text),
    CONSTRAINT limit_order_actions_positive_input CHECK (input_amount <> '0'::text),
    CONSTRAINT limit_order_actions_positive_output CHECK (output_amount <> '0'::text),
    CONSTRAINT limit_order_actions_residual_policy_check CHECK (residual_policy = 'refund'::text)
);

ALTER TABLE ONLY public.limit_order_actions
    ADD CONSTRAINT limit_order_actions_pkey PRIMARY KEY (order_id);

ALTER TABLE ONLY public.limit_order_actions
    ADD CONSTRAINT limit_order_actions_order_id_fkey
        FOREIGN KEY (order_id) REFERENCES public.router_orders(id) ON DELETE CASCADE;

CREATE TRIGGER update_limit_order_actions_updated_at
    BEFORE UPDATE ON public.limit_order_actions
    FOR EACH ROW
    EXECUTE FUNCTION public.update_updated_at_column();

CREATE TABLE public.limit_order_quotes (
    id uuid NOT NULL,
    order_id uuid,
    provider_id text NOT NULL,
    input_amount text NOT NULL,
    output_amount text NOT NULL,
    residual_policy text DEFAULT 'refund'::text NOT NULL,
    provider_quote jsonb DEFAULT '{}'::jsonb NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    source_chain_id text NOT NULL,
    source_asset_id text NOT NULL,
    destination_chain_id text NOT NULL,
    destination_asset_id text NOT NULL,
    recipient_address text NOT NULL,
    CONSTRAINT limit_order_quotes_destination_asset_not_empty CHECK (destination_asset_id <> ''::text),
    CONSTRAINT limit_order_quotes_destination_chain_not_empty CHECK (destination_chain_id <> ''::text),
    CONSTRAINT limit_order_quotes_input_amount_digits CHECK (input_amount ~ '^[0-9]+$'::text),
    CONSTRAINT limit_order_quotes_output_amount_digits CHECK (output_amount ~ '^[0-9]+$'::text),
    CONSTRAINT limit_order_quotes_positive_input CHECK (input_amount <> '0'::text),
    CONSTRAINT limit_order_quotes_positive_output CHECK (output_amount <> '0'::text),
    CONSTRAINT limit_order_quotes_provider_id_not_empty CHECK (provider_id <> ''::text),
    CONSTRAINT limit_order_quotes_provider_quote_object CHECK (jsonb_typeof(provider_quote) = 'object'::text),
    CONSTRAINT limit_order_quotes_recipient_address_not_empty CHECK (recipient_address <> ''::text),
    CONSTRAINT limit_order_quotes_residual_policy_check CHECK (residual_policy = 'refund'::text),
    CONSTRAINT limit_order_quotes_source_asset_not_empty CHECK (source_asset_id <> ''::text),
    CONSTRAINT limit_order_quotes_source_chain_not_empty CHECK (source_chain_id <> ''::text)
);

ALTER TABLE ONLY public.limit_order_quotes
    ADD CONSTRAINT limit_order_quotes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.limit_order_quotes
    ADD CONSTRAINT limit_order_quotes_order_id_fkey
        FOREIGN KEY (order_id) REFERENCES public.router_orders(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX idx_limit_order_quotes_order_id
    ON public.limit_order_quotes USING btree (order_id);

CREATE INDEX idx_limit_order_quotes_expires_at
    ON public.limit_order_quotes USING btree (expires_at);

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
                    'hyperliquid_bridge_deposit'::text,
                    'hyperliquid_bridge_withdrawal'::text,
                    'universal_router_swap'::text,
                    'refund'::text
                ]
            )
        );

ALTER TABLE public.order_provider_operations
    DROP CONSTRAINT IF EXISTS order_provider_operations_type_check;

ALTER TABLE public.order_provider_operations
    ADD CONSTRAINT order_provider_operations_type_check
        CHECK (
            operation_type = ANY (
                ARRAY[
                    'across_bridge'::text,
                    'cctp_bridge'::text,
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
