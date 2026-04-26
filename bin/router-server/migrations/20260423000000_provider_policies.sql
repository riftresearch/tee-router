CREATE TABLE provider_policies (
    provider TEXT PRIMARY KEY,
    quote_state TEXT NOT NULL CHECK (quote_state IN ('enabled', 'disabled')),
    execution_state TEXT NOT NULL CHECK (execution_state IN ('enabled', 'drain', 'disabled')),
    reason TEXT NOT NULL DEFAULT '',
    updated_by TEXT NOT NULL DEFAULT 'unknown',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
