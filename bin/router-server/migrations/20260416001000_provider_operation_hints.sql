CREATE TABLE order_provider_operation_hints (
    id UUID PRIMARY KEY,
    provider_operation_id UUID NOT NULL REFERENCES order_provider_operations(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    hint_kind TEXT NOT NULL,
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    idempotency_key TEXT,
    error_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    claimed_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT order_provider_operation_hints_source_not_empty CHECK (source <> ''),
    CONSTRAINT order_provider_operation_hints_kind_check CHECK (
        hint_kind IN ('possible_progress')
    ),
    CONSTRAINT order_provider_operation_hints_status_check CHECK (
        status IN ('pending', 'processing', 'processed', 'ignored', 'failed')
    ),
    CONSTRAINT order_provider_operation_hints_idempotency_key_not_empty CHECK (
        idempotency_key IS NULL OR idempotency_key <> ''
    ),
    CONSTRAINT order_provider_operation_hints_evidence_object CHECK (
        jsonb_typeof(evidence_json) = 'object'
    ),
    CONSTRAINT order_provider_operation_hints_error_object CHECK (
        jsonb_typeof(error_json) = 'object'
    )
);

CREATE UNIQUE INDEX idx_order_provider_operation_hints_idempotency_key
    ON order_provider_operation_hints (source, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_order_provider_operation_hints_pending
    ON order_provider_operation_hints (status, created_at)
    WHERE status IN ('pending', 'processing');

CREATE INDEX idx_order_provider_operation_hints_operation_id
    ON order_provider_operation_hints (provider_operation_id, created_at);

CREATE TRIGGER update_order_provider_operation_hints_updated_at BEFORE UPDATE ON order_provider_operation_hints
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
