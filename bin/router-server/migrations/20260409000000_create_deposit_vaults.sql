CREATE TABLE deposit_vaults (
    id UUID PRIMARY KEY,
    deposit_chain VARCHAR(50) NOT NULL,
    deposit_token JSONB NOT NULL,
    deposit_decimals SMALLINT NOT NULL,
    action JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    deposit_vault_salt BYTEA NOT NULL,
    deposit_vault_address VARCHAR(255) NOT NULL,
    recovery_address VARCHAR(255) NOT NULL,
    cancellation_commitment BYTEA NOT NULL,
    cancel_after TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    refund_requested_at TIMESTAMPTZ,
    refunded_at TIMESTAMPTZ,
    refund_tx_hash TEXT,
    last_refund_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT deposit_vault_salt_len CHECK (octet_length(deposit_vault_salt) = 32),
    CONSTRAINT deposit_vault_cancellation_commitment_len CHECK (octet_length(cancellation_commitment) = 32),
    CONSTRAINT deposit_vault_status_check CHECK (
        status IN (
            'pending_funding',
            'funded',
            'executing',
            'completed',
            'refund_required',
            'refunding',
            'refunded',
            'refund_manual_intervention_required'
        )
    )
);

CREATE UNIQUE INDEX idx_deposit_vaults_chain_address
    ON deposit_vaults (deposit_chain, deposit_vault_address);

CREATE INDEX idx_deposit_vaults_status
    ON deposit_vaults (status);

CREATE INDEX idx_deposit_vaults_created_at
    ON deposit_vaults (created_at DESC);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_deposit_vaults_updated_at BEFORE UPDATE ON deposit_vaults
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();
