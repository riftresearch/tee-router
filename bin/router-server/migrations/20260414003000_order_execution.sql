CREATE TABLE market_order_actions (
    order_id UUID PRIMARY KEY REFERENCES router_orders(id) ON DELETE CASCADE,
    order_kind TEXT NOT NULL,
    amount_in TEXT,
    min_amount_out TEXT,
    amount_out TEXT,
    max_amount_in TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT market_order_actions_order_kind_check CHECK (
        order_kind IN (
            'exact_in',
            'exact_out'
        )
    ),
    CONSTRAINT market_order_actions_shape_check CHECK (
        (
            order_kind = 'exact_in'
            AND amount_in IS NOT NULL
            AND min_amount_out IS NOT NULL
            AND amount_out IS NULL
            AND max_amount_in IS NULL
        )
        OR (
            order_kind = 'exact_out'
            AND amount_in IS NULL
            AND min_amount_out IS NULL
            AND amount_out IS NOT NULL
            AND max_amount_in IS NOT NULL
        )
    ),
    CONSTRAINT market_order_actions_amount_in_digits CHECK (
        amount_in IS NULL OR amount_in ~ '^[0-9]+$'
    ),
    CONSTRAINT market_order_actions_min_amount_out_digits CHECK (
        min_amount_out IS NULL OR min_amount_out ~ '^[0-9]+$'
    ),
    CONSTRAINT market_order_actions_amount_out_digits CHECK (
        amount_out IS NULL OR amount_out ~ '^[0-9]+$'
    ),
    CONSTRAINT market_order_actions_max_amount_in_digits CHECK (
        max_amount_in IS NULL OR max_amount_in ~ '^[0-9]+$'
    )
);

INSERT INTO market_order_actions (
    order_id,
    order_kind,
    amount_in,
    min_amount_out,
    amount_out,
    max_amount_in,
    created_at,
    updated_at
)
SELECT
    id,
    action_payload->'payload'->>'kind',
    action_payload->'payload'->>'amount_in',
    action_payload->'payload'->>'min_amount_out',
    action_payload->'payload'->>'amount_out',
    action_payload->'payload'->>'max_amount_in',
    created_at,
    updated_at
FROM router_orders
WHERE order_type = 'market_order';

ALTER TABLE router_orders
    DROP COLUMN action_payload;

CREATE TABLE order_custody_accounts (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES router_orders(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    chain_id TEXT NOT NULL,
    address TEXT NOT NULL,
    signer_ref TEXT,
    status TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT order_custody_accounts_role_check CHECK (
        role IN (
            'source_deposit',
            'across_refund',
            'destination_execution',
            'unit_recovery'
        )
    ),
    CONSTRAINT order_custody_accounts_status_check CHECK (
        status IN (
            'planned',
            'active',
            'released',
            'failed'
        )
    ),
    CONSTRAINT order_custody_accounts_chain_id_not_empty CHECK (chain_id <> ''),
    CONSTRAINT order_custody_accounts_address_not_empty CHECK (address <> ''),
    CONSTRAINT order_custody_accounts_signer_ref_not_empty CHECK (
        signer_ref IS NULL OR signer_ref <> ''
    ),
    CONSTRAINT order_custody_accounts_metadata_object CHECK (
        jsonb_typeof(metadata_json) = 'object'
    )
);

CREATE UNIQUE INDEX idx_order_custody_accounts_order_role_chain
    ON order_custody_accounts (order_id, role, chain_id);

CREATE INDEX idx_order_custody_accounts_order_id
    ON order_custody_accounts (order_id);

CREATE INDEX idx_order_custody_accounts_chain_address
    ON order_custody_accounts (chain_id, address);

CREATE TABLE order_execution_steps (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES router_orders(id) ON DELETE CASCADE,
    transition_decl_id TEXT,
    step_index INTEGER NOT NULL,
    step_type TEXT NOT NULL,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
    input_chain_id TEXT,
    input_asset_id TEXT,
    output_chain_id TEXT,
    output_asset_id TEXT,
    amount_in TEXT,
    min_amount_out TEXT,
    tx_hash TEXT,
    provider_ref TEXT,
    idempotency_key TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT order_execution_steps_step_index_nonnegative CHECK (step_index >= 0),
    CONSTRAINT order_execution_steps_step_type_check CHECK (
        step_type IN (
            'wait_for_deposit',
            'across_bridge',
            'unit_deposit',
            'hyperliquid_trade',
            'refund'
        )
    ),
    CONSTRAINT order_execution_steps_status_check CHECK (
        status IN (
            'planned',
            'waiting',
            'ready',
            'running',
            'completed',
            'failed',
            'skipped',
            'cancelled'
        )
    ),
    CONSTRAINT order_execution_steps_provider_not_empty CHECK (provider <> ''),
    CONSTRAINT order_execution_steps_input_asset_pair CHECK (
        (input_chain_id IS NULL AND input_asset_id IS NULL)
        OR (input_chain_id IS NOT NULL AND input_asset_id IS NOT NULL)
    ),
    CONSTRAINT order_execution_steps_output_asset_pair CHECK (
        (output_chain_id IS NULL AND output_asset_id IS NULL)
        OR (output_chain_id IS NOT NULL AND output_asset_id IS NOT NULL)
    ),
    CONSTRAINT order_execution_steps_amount_in_digits CHECK (
        amount_in IS NULL OR amount_in ~ '^[0-9]+$'
    ),
    CONSTRAINT order_execution_steps_min_amount_out_digits CHECK (
        min_amount_out IS NULL OR min_amount_out ~ '^[0-9]+$'
    ),
    CONSTRAINT order_execution_steps_tx_hash_not_empty CHECK (
        tx_hash IS NULL OR tx_hash <> ''
    ),
    CONSTRAINT order_execution_steps_provider_ref_not_empty CHECK (
        provider_ref IS NULL OR provider_ref <> ''
    ),
    CONSTRAINT order_execution_steps_transition_decl_id_not_empty CHECK (
        transition_decl_id IS NULL OR transition_decl_id <> ''
    ),
    CONSTRAINT order_execution_steps_idempotency_key_not_empty CHECK (
        idempotency_key IS NULL OR idempotency_key <> ''
    ),
    CONSTRAINT order_execution_steps_attempt_count_nonnegative CHECK (attempt_count >= 0),
    CONSTRAINT order_execution_steps_details_object CHECK (
        jsonb_typeof(details_json) = 'object'
    ),
    CONSTRAINT order_execution_steps_request_object CHECK (
        jsonb_typeof(request_json) = 'object'
    ),
    CONSTRAINT order_execution_steps_response_object CHECK (
        jsonb_typeof(response_json) = 'object'
    ),
    CONSTRAINT order_execution_steps_error_object CHECK (
        jsonb_typeof(error_json) = 'object'
    ),
    CONSTRAINT order_execution_steps_lifecycle_timestamps CHECK (
        completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at
    )
);

CREATE UNIQUE INDEX idx_order_execution_steps_order_index
    ON order_execution_steps (order_id, step_index);

CREATE UNIQUE INDEX idx_order_execution_steps_provider_ref
    ON order_execution_steps (provider, provider_ref)
    WHERE provider_ref IS NOT NULL;

CREATE INDEX idx_order_execution_steps_order_id
    ON order_execution_steps (order_id);

CREATE INDEX idx_order_execution_steps_status_next_attempt
    ON order_execution_steps (status, next_attempt_at)
    WHERE status IN ('planned', 'waiting', 'ready', 'running');

CREATE TRIGGER update_market_order_actions_updated_at BEFORE UPDATE ON market_order_actions
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_order_custody_accounts_updated_at BEFORE UPDATE ON order_custody_accounts
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_order_execution_steps_updated_at BEFORE UPDATE ON order_execution_steps
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
