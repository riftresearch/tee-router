CREATE TABLE IF NOT EXISTS hl_transfers (
    user_addr        BYTEA NOT NULL,
    time_ms          BIGINT NOT NULL,
    kind             TEXT NOT NULL,
    canonical_key    TEXT NOT NULL,
    asset            TEXT NOT NULL,
    market           TEXT,
    amount_delta     NUMERIC(78, 18) NOT NULL,
    fee              NUMERIC(78, 18),
    fee_token        TEXT,
    hash             BYTEA NOT NULL,
    metadata         JSONB NOT NULL,
    observed_at_ms   BIGINT NOT NULL,
    UNIQUE (user_addr, canonical_key)
) PARTITION BY HASH (user_addr);

CREATE TABLE IF NOT EXISTS hl_transfers_0 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 0);
CREATE TABLE IF NOT EXISTS hl_transfers_1 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 1);
CREATE TABLE IF NOT EXISTS hl_transfers_2 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 2);
CREATE TABLE IF NOT EXISTS hl_transfers_3 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 3);
CREATE TABLE IF NOT EXISTS hl_transfers_4 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 4);
CREATE TABLE IF NOT EXISTS hl_transfers_5 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 5);
CREATE TABLE IF NOT EXISTS hl_transfers_6 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 6);
CREATE TABLE IF NOT EXISTS hl_transfers_7 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 7);
CREATE TABLE IF NOT EXISTS hl_transfers_8 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 8);
CREATE TABLE IF NOT EXISTS hl_transfers_9 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 9);
CREATE TABLE IF NOT EXISTS hl_transfers_10 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 10);
CREATE TABLE IF NOT EXISTS hl_transfers_11 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 11);
CREATE TABLE IF NOT EXISTS hl_transfers_12 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 12);
CREATE TABLE IF NOT EXISTS hl_transfers_13 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 13);
CREATE TABLE IF NOT EXISTS hl_transfers_14 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 14);
CREATE TABLE IF NOT EXISTS hl_transfers_15 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 15);

CREATE INDEX IF NOT EXISTS idx_hl_transfers_user_time ON hl_transfers (user_addr, time_ms);
CREATE INDEX IF NOT EXISTS idx_hl_transfers_oid ON hl_transfers ((metadata->>'oid')) WHERE kind = 'fill';

CREATE TABLE IF NOT EXISTS hl_orders (
    user_addr             BYTEA NOT NULL,
    oid                   BIGINT NOT NULL,
    cloid                 TEXT,
    coin                  TEXT NOT NULL,
    side                  CHAR(1) NOT NULL,
    limit_px              TEXT NOT NULL,
    sz                    TEXT NOT NULL,
    orig_sz               TEXT NOT NULL,
    status                TEXT NOT NULL,
    status_timestamp_ms   BIGINT NOT NULL,
    observed_at_ms        BIGINT NOT NULL,
    PRIMARY KEY (user_addr, oid)
);

CREATE INDEX IF NOT EXISTS idx_hl_orders_user_time ON hl_orders (user_addr, status_timestamp_ms);

CREATE TABLE IF NOT EXISTS hl_cursors (
    user_addr  BYTEA NOT NULL,
    endpoint   TEXT NOT NULL,
    cursor_ms  BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_addr, endpoint)
);
