CREATE TABLE deposit_vault_funding_hints (
    id UUID PRIMARY KEY,
    vault_id UUID NOT NULL REFERENCES deposit_vaults(id) ON DELETE CASCADE,
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
    CONSTRAINT deposit_vault_funding_hints_source_not_empty CHECK (source <> ''),
    CONSTRAINT deposit_vault_funding_hints_kind_check CHECK (
        hint_kind IN ('possible_progress')
    ),
    CONSTRAINT deposit_vault_funding_hints_status_check CHECK (
        status IN ('pending', 'processing', 'processed', 'ignored', 'failed')
    ),
    CONSTRAINT deposit_vault_funding_hints_idempotency_key_not_empty CHECK (
        idempotency_key IS NULL OR idempotency_key <> ''
    ),
    CONSTRAINT deposit_vault_funding_hints_evidence_object CHECK (
        jsonb_typeof(evidence_json) = 'object'
    ),
    CONSTRAINT deposit_vault_funding_hints_error_object CHECK (
        jsonb_typeof(error_json) = 'object'
    )
);

CREATE UNIQUE INDEX idx_deposit_vault_funding_hints_idempotency_key
    ON deposit_vault_funding_hints (source, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_deposit_vault_funding_hints_pending
    ON deposit_vault_funding_hints (status, created_at)
    WHERE status IN ('pending', 'processing');

CREATE INDEX idx_deposit_vault_funding_hints_vault_id
    ON deposit_vault_funding_hints (vault_id, created_at);

CREATE TRIGGER update_deposit_vault_funding_hints_updated_at BEFORE UPDATE ON deposit_vault_funding_hints
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
