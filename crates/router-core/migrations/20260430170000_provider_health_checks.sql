CREATE TABLE provider_health_checks (
    provider text PRIMARY KEY,
    status text NOT NULL,
    checked_at timestamp with time zone NOT NULL,
    latency_ms bigint,
    http_status integer,
    error text,
    updated_by text DEFAULT 'unknown'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT provider_health_checks_status_check CHECK (status = ANY (ARRAY['ok'::text, 'down'::text, 'unknown'::text])),
    CONSTRAINT provider_health_checks_latency_ms_check CHECK (latency_ms IS NULL OR latency_ms >= 0),
    CONSTRAINT provider_health_checks_http_status_check CHECK (http_status IS NULL OR (http_status >= 100 AND http_status <= 599))
);

CREATE INDEX idx_provider_health_checks_status ON provider_health_checks (status);
CREATE INDEX idx_provider_health_checks_checked_at ON provider_health_checks (checked_at);
