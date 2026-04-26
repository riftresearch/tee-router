CREATE TABLE order_provider_operations (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES router_orders(id) ON DELETE CASCADE,
    execution_step_id UUID REFERENCES order_execution_steps(id) ON DELETE SET NULL,
    provider TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    provider_ref TEXT,
    status TEXT NOT NULL,
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    observed_state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT order_provider_operations_provider_not_empty CHECK (provider <> ''),
    CONSTRAINT order_provider_operations_type_check CHECK (
        operation_type IN (
            'across_bridge',
            'unit_deposit',
            'unit_withdrawal',
            'hyperliquid_trade'
        )
    ),
    CONSTRAINT order_provider_operations_status_check CHECK (
        status IN (
            'planned',
            'submitted',
            'waiting_external',
            'completed',
            'failed',
            'expired'
        )
    ),
    CONSTRAINT order_provider_operations_provider_ref_not_empty CHECK (
        provider_ref IS NULL OR provider_ref <> ''
    ),
    CONSTRAINT order_provider_operations_request_object CHECK (
        jsonb_typeof(request_json) = 'object'
    ),
    CONSTRAINT order_provider_operations_response_object CHECK (
        jsonb_typeof(response_json) = 'object'
    ),
    CONSTRAINT order_provider_operations_observed_state_object CHECK (
        jsonb_typeof(observed_state_json) = 'object'
    )
);

CREATE UNIQUE INDEX idx_order_provider_operations_provider_ref
    ON order_provider_operations (provider, provider_ref)
    WHERE provider_ref IS NOT NULL;

CREATE INDEX idx_order_provider_operations_order_id
    ON order_provider_operations (order_id);

CREATE INDEX idx_order_provider_operations_step_id
    ON order_provider_operations (execution_step_id)
    WHERE execution_step_id IS NOT NULL;

CREATE INDEX idx_order_provider_operations_status
    ON order_provider_operations (status, updated_at)
    WHERE status IN ('planned', 'submitted', 'waiting_external');

CREATE TABLE order_execution_attempts (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES router_orders(id) ON DELETE CASCADE,
    attempt_index INTEGER NOT NULL,
    attempt_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    trigger_step_id UUID REFERENCES order_execution_steps(id) ON DELETE SET NULL,
    trigger_provider_operation_id UUID REFERENCES order_provider_operations(id) ON DELETE SET NULL,
    failure_reason_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_custody_snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT order_execution_attempts_attempt_index_positive CHECK (attempt_index > 0),
    CONSTRAINT order_execution_attempts_attempt_kind_check CHECK (
        attempt_kind IN (
            'primary_execution',
            'retry_execution',
            'refund_recovery'
        )
    ),
    CONSTRAINT order_execution_attempts_status_check CHECK (
        status IN (
            'planning',
            'active',
            'completed',
            'failed',
            'refund_required',
            'manual_intervention_required'
        )
    ),
    CONSTRAINT order_execution_attempts_failure_reason_object CHECK (
        jsonb_typeof(failure_reason_json) = 'object'
    ),
    CONSTRAINT order_execution_attempts_input_custody_snapshot_object CHECK (
        jsonb_typeof(input_custody_snapshot_json) = 'object'
    )
);

CREATE UNIQUE INDEX idx_order_execution_attempts_order_attempt_index
    ON order_execution_attempts (order_id, attempt_index);

CREATE UNIQUE INDEX idx_order_execution_attempts_order_active
    ON order_execution_attempts (order_id)
    WHERE status IN ('planning', 'active');

CREATE INDEX idx_order_execution_attempts_order_id
    ON order_execution_attempts (order_id);

CREATE INDEX idx_order_execution_attempts_status_updated_at
    ON order_execution_attempts (status, updated_at);

CREATE TABLE paymaster_gas_ledger (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES router_orders(id) ON DELETE CASCADE,
    execution_attempt_id UUID REFERENCES order_execution_attempts(id) ON DELETE SET NULL,
    execution_step_id UUID REFERENCES order_execution_steps(id) ON DELETE SET NULL,
    debt_id TEXT NOT NULL,
    spend_chain_id TEXT NOT NULL,
    estimated_native_gas_wei TEXT NOT NULL,
    estimated_usd_micro TEXT NOT NULL,
    settlement_chain_id TEXT NOT NULL,
    settlement_asset_id TEXT NOT NULL,
    settlement_amount TEXT NOT NULL,
    settlement_tx_hash TEXT,
    actual_native_gas_wei TEXT,
    actual_usd_micro TEXT,
    status TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT paymaster_gas_ledger_debt_id_not_empty CHECK (debt_id <> ''),
    CONSTRAINT paymaster_gas_ledger_spend_chain_not_empty CHECK (spend_chain_id <> ''),
    CONSTRAINT paymaster_gas_ledger_estimated_native_digits CHECK (estimated_native_gas_wei ~ '^[0-9]+$'),
    CONSTRAINT paymaster_gas_ledger_estimated_usd_digits CHECK (estimated_usd_micro ~ '^[0-9]+$'),
    CONSTRAINT paymaster_gas_ledger_settlement_chain_not_empty CHECK (settlement_chain_id <> ''),
    CONSTRAINT paymaster_gas_ledger_settlement_asset_not_empty CHECK (settlement_asset_id <> ''),
    CONSTRAINT paymaster_gas_ledger_settlement_amount_digits CHECK (settlement_amount ~ '^[0-9]+$'),
    CONSTRAINT paymaster_gas_ledger_settlement_tx_hash_not_empty CHECK (
        settlement_tx_hash IS NULL OR settlement_tx_hash <> ''
    ),
    CONSTRAINT paymaster_gas_ledger_actual_native_digits CHECK (
        actual_native_gas_wei IS NULL OR actual_native_gas_wei ~ '^[0-9]+$'
    ),
    CONSTRAINT paymaster_gas_ledger_actual_usd_digits CHECK (
        actual_usd_micro IS NULL OR actual_usd_micro ~ '^[0-9]+$'
    ),
    CONSTRAINT paymaster_gas_ledger_status_check CHECK (
        status IN (
            'quoted',
            'retained',
            'actualized',
            'rebate_due',
            'shortfall_due',
            'void'
        )
    ),
    CONSTRAINT paymaster_gas_ledger_metadata_object CHECK (
        jsonb_typeof(metadata_json) = 'object'
    )
);

CREATE INDEX idx_paymaster_gas_ledger_order_id
    ON paymaster_gas_ledger (order_id);

CREATE INDEX idx_paymaster_gas_ledger_step_id
    ON paymaster_gas_ledger (execution_step_id)
    WHERE execution_step_id IS NOT NULL;

CREATE INDEX idx_paymaster_gas_ledger_status
    ON paymaster_gas_ledger (status, updated_at);

ALTER TABLE order_execution_steps
    ADD COLUMN execution_attempt_id UUID REFERENCES order_execution_attempts(id) ON DELETE CASCADE;

ALTER TABLE order_provider_operations
    ADD COLUMN execution_attempt_id UUID REFERENCES order_execution_attempts(id) ON DELETE SET NULL;

DROP INDEX idx_order_execution_steps_order_index;
DROP INDEX idx_order_execution_steps_provider_ref;

CREATE UNIQUE INDEX idx_order_execution_steps_order_wait_step
    ON order_execution_steps (order_id, step_index)
    WHERE execution_attempt_id IS NULL;

CREATE UNIQUE INDEX idx_order_execution_steps_attempt_index
    ON order_execution_steps (execution_attempt_id, step_index)
    WHERE execution_attempt_id IS NOT NULL;

CREATE INDEX idx_order_execution_steps_attempt_id
    ON order_execution_steps (execution_attempt_id)
    WHERE execution_attempt_id IS NOT NULL;

CREATE TABLE order_provider_addresses (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES router_orders(id) ON DELETE CASCADE,
    execution_step_id UUID REFERENCES order_execution_steps(id) ON DELETE SET NULL,
    provider_operation_id UUID REFERENCES order_provider_operations(id) ON DELETE SET NULL,
    provider TEXT NOT NULL,
    role TEXT NOT NULL,
    chain_id TEXT NOT NULL,
    asset_id TEXT,
    address TEXT NOT NULL,
    memo TEXT,
    expires_at TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT order_provider_addresses_provider_not_empty CHECK (provider <> ''),
    CONSTRAINT order_provider_addresses_role_check CHECK (
        role IN (
            'across_recipient',
            'across_refund',
            'unit_deposit',
            'unit_revert',
            'hyperliquid_destination'
        )
    ),
    CONSTRAINT order_provider_addresses_chain_id_not_empty CHECK (chain_id <> ''),
    CONSTRAINT order_provider_addresses_asset_id_not_empty CHECK (
        asset_id IS NULL OR asset_id <> ''
    ),
    CONSTRAINT order_provider_addresses_address_not_empty CHECK (address <> ''),
    CONSTRAINT order_provider_addresses_memo_not_empty CHECK (
        memo IS NULL OR memo <> ''
    ),
    CONSTRAINT order_provider_addresses_metadata_object CHECK (
        jsonb_typeof(metadata_json) = 'object'
    )
);

CREATE INDEX idx_order_provider_addresses_order_id
    ON order_provider_addresses (order_id);

CREATE INDEX idx_order_provider_addresses_step_id
    ON order_provider_addresses (execution_step_id)
    WHERE execution_step_id IS NOT NULL;

CREATE INDEX idx_order_provider_addresses_operation_id
    ON order_provider_addresses (provider_operation_id)
    WHERE provider_operation_id IS NOT NULL;

CREATE INDEX idx_order_provider_addresses_chain_address
    ON order_provider_addresses (chain_id, address);

CREATE UNIQUE INDEX idx_order_provider_addresses_order_provider_role_chain_address
    ON order_provider_addresses (order_id, provider, role, chain_id, address);

CREATE TRIGGER update_order_provider_operations_updated_at BEFORE UPDATE ON order_provider_operations
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_order_provider_addresses_updated_at BEFORE UPDATE ON order_provider_addresses
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_order_execution_attempts_updated_at BEFORE UPDATE ON order_execution_attempts
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_paymaster_gas_ledger_updated_at BEFORE UPDATE ON paymaster_gas_ledger
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
