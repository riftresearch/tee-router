CREATE TABLE router_orders (
    id UUID PRIMARY KEY,
    order_type TEXT NOT NULL,
    status TEXT NOT NULL,
    funding_vault_id UUID REFERENCES deposit_vaults(id),
    source_chain_id TEXT NOT NULL,
    source_asset_id TEXT NOT NULL,
    destination_chain_id TEXT NOT NULL,
    destination_asset_id TEXT NOT NULL,
    recipient_address TEXT NOT NULL,
    refund_address TEXT NOT NULL,
    action_payload JSONB NOT NULL,
    action_timeout_at TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT router_orders_order_type_check CHECK (
        order_type IN (
            'market_order'
        )
    ),
    CONSTRAINT router_orders_status_check CHECK (
        status IN (
            'quoted',
            'pending_funding',
            'funded',
            'executing',
            'completed',
            'refund_required',
            'refunding',
            'refunded',
            'refund_manual_intervention_required',
            'failed',
            'expired'
        )
    ),
    CONSTRAINT router_orders_source_chain_not_empty CHECK (source_chain_id <> ''),
    CONSTRAINT router_orders_source_asset_not_empty CHECK (source_asset_id <> ''),
    CONSTRAINT router_orders_destination_chain_not_empty CHECK (destination_chain_id <> ''),
    CONSTRAINT router_orders_destination_asset_not_empty CHECK (destination_asset_id <> ''),
    CONSTRAINT router_orders_recipient_address_not_empty CHECK (recipient_address <> ''),
    CONSTRAINT router_orders_refund_address_not_empty CHECK (refund_address <> ''),
    CONSTRAINT router_orders_idempotency_key_not_empty CHECK (
        idempotency_key IS NULL OR idempotency_key <> ''
    )
);

CREATE UNIQUE INDEX idx_router_orders_idempotency_key
    ON router_orders (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_router_orders_status_timeout
    ON router_orders (status, action_timeout_at);

CREATE INDEX idx_router_orders_funding_vault_id
    ON router_orders (funding_vault_id)
    WHERE funding_vault_id IS NOT NULL;

CREATE TABLE market_order_quotes (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES router_orders(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL,
    order_kind TEXT NOT NULL,
    amount_in TEXT NOT NULL,
    amount_out TEXT NOT NULL,
    min_amount_out TEXT,
    max_amount_in TEXT,
    provider_quote JSONB NOT NULL DEFAULT '{}'::jsonb,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT market_order_quotes_provider_id_not_empty CHECK (provider_id <> ''),
    CONSTRAINT market_order_quotes_order_kind_check CHECK (
        order_kind IN (
            'exact_in',
            'exact_out'
        )
    ),
    CONSTRAINT market_order_quotes_amount_in_digits CHECK (amount_in ~ '^[0-9]+$'),
    CONSTRAINT market_order_quotes_amount_out_digits CHECK (amount_out ~ '^[0-9]+$'),
    CONSTRAINT market_order_quotes_min_amount_out_digits CHECK (
        min_amount_out IS NULL OR min_amount_out ~ '^[0-9]+$'
    ),
    CONSTRAINT market_order_quotes_max_amount_in_digits CHECK (
        max_amount_in IS NULL OR max_amount_in ~ '^[0-9]+$'
    )
);

CREATE UNIQUE INDEX idx_market_order_quotes_order_id
    ON market_order_quotes (order_id);

CREATE INDEX idx_market_order_quotes_expires_at
    ON market_order_quotes (expires_at);

ALTER TABLE deposit_vaults
    ADD COLUMN order_id UUID REFERENCES router_orders(id);

CREATE INDEX idx_deposit_vaults_order_id
    ON deposit_vaults (order_id)
    WHERE order_id IS NOT NULL;

CREATE TRIGGER update_router_orders_updated_at BEFORE UPDATE ON router_orders
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
