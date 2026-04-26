ALTER TABLE deposit_vaults
    ADD COLUMN refund_attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_refund_attempt_at TIMESTAMPTZ,
    ADD COLUMN refund_next_attempt_at TIMESTAMPTZ,
    ADD COLUMN refund_claimed_by TEXT,
    ADD COLUMN refund_claimed_until TIMESTAMPTZ;

CREATE INDEX idx_deposit_vaults_timeout_refund_claimable
    ON deposit_vaults (cancel_after, status)
    WHERE status IN ('pending_funding', 'funded');

CREATE INDEX idx_deposit_vaults_refunding_claimable
    ON deposit_vaults (status, refund_next_attempt_at, refund_claimed_until, updated_at)
    WHERE status = 'refunding';
