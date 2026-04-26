CREATE TABLE router_route_cost_snapshots (
    transition_id TEXT NOT NULL,
    amount_bucket TEXT NOT NULL,
    provider TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    source_chain TEXT NOT NULL,
    source_asset TEXT NOT NULL,
    destination_chain TEXT NOT NULL,
    destination_asset TEXT NOT NULL,
    estimated_fee_bps BIGINT NOT NULL CHECK (estimated_fee_bps >= 0),
    estimated_gas_usd_micros BIGINT NOT NULL CHECK (estimated_gas_usd_micros >= 0),
    estimated_latency_ms BIGINT NOT NULL CHECK (estimated_latency_ms >= 0),
    sample_amount_usd_micros BIGINT NOT NULL CHECK (sample_amount_usd_micros > 0),
    quote_source TEXT NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (transition_id, amount_bucket)
);

CREATE INDEX idx_router_route_cost_snapshots_active
    ON router_route_cost_snapshots (expires_at, amount_bucket);
