CREATE TABLE public.router_swap_time_averages (
    provider text NOT NULL,
    leg_type text NOT NULL,
    transition_decl_id text NOT NULL,
    input_chain_id text NOT NULL,
    input_asset_id text NOT NULL,
    output_chain_id text NOT NULL,
    output_asset_id text NOT NULL,
    sample_count bigint NOT NULL,
    total_duration_ms bigint NOT NULL,
    avg_duration_ms bigint NOT NULL,
    min_duration_ms bigint NOT NULL,
    max_duration_ms bigint NOT NULL,
    last_sample_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT router_swap_time_averages_pkey PRIMARY KEY (
        provider,
        leg_type,
        transition_decl_id,
        input_chain_id,
        input_asset_id,
        output_chain_id,
        output_asset_id
    ),
    CONSTRAINT router_swap_time_averages_provider_not_empty CHECK (provider <> ''::text),
    CONSTRAINT router_swap_time_averages_leg_type_not_empty CHECK (leg_type <> ''::text),
    CONSTRAINT router_swap_time_averages_transition_decl_id_not_empty CHECK (transition_decl_id <> ''::text),
    CONSTRAINT router_swap_time_averages_input_chain_id_not_empty CHECK (input_chain_id <> ''::text),
    CONSTRAINT router_swap_time_averages_input_asset_id_not_empty CHECK (input_asset_id <> ''::text),
    CONSTRAINT router_swap_time_averages_output_chain_id_not_empty CHECK (output_chain_id <> ''::text),
    CONSTRAINT router_swap_time_averages_output_asset_id_not_empty CHECK (output_asset_id <> ''::text),
    CONSTRAINT router_swap_time_averages_sample_count_positive CHECK (sample_count > 0),
    CONSTRAINT router_swap_time_averages_duration_nonnegative CHECK (
        total_duration_ms >= 0
        AND avg_duration_ms >= 0
        AND min_duration_ms >= 0
        AND max_duration_ms >= 0
        AND max_duration_ms >= min_duration_ms
    )
);

CREATE INDEX idx_router_swap_time_averages_last_sample
    ON public.router_swap_time_averages USING btree (last_sample_at DESC, updated_at DESC);

INSERT INTO public.router_swap_time_averages (
    provider,
    leg_type,
    transition_decl_id,
    input_chain_id,
    input_asset_id,
    output_chain_id,
    output_asset_id,
    sample_count,
    total_duration_ms,
    avg_duration_ms,
    min_duration_ms,
    max_duration_ms,
    last_sample_at,
    created_at,
    updated_at
)
SELECT
    samples.provider,
    samples.leg_type,
    samples.transition_decl_id,
    samples.input_chain_id,
    samples.input_asset_id,
    samples.output_chain_id,
    samples.output_asset_id,
    COUNT(*)::bigint AS sample_count,
    SUM(samples.duration_ms)::bigint AS total_duration_ms,
    ROUND(AVG(samples.duration_ms))::bigint AS avg_duration_ms,
    MIN(samples.duration_ms)::bigint AS min_duration_ms,
    MAX(samples.duration_ms)::bigint AS max_duration_ms,
    MAX(samples.completed_at) AS last_sample_at,
    now() AS created_at,
    now() AS updated_at
FROM (
    SELECT
        provider,
        leg_type,
        transition_decl_id,
        input_chain_id,
        input_asset_id,
        output_chain_id,
        output_asset_id,
        ROUND(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)::bigint AS duration_ms,
        completed_at
    FROM public.order_execution_legs
    WHERE status = 'completed'
      AND transition_decl_id IS NOT NULL
      AND started_at IS NOT NULL
      AND completed_at IS NOT NULL
      AND completed_at >= started_at
) samples
GROUP BY
    samples.provider,
    samples.leg_type,
    samples.transition_decl_id,
    samples.input_chain_id,
    samples.input_asset_id,
    samples.output_chain_id,
    samples.output_asset_id
ON CONFLICT (
    provider,
    leg_type,
    transition_decl_id,
    input_chain_id,
    input_asset_id,
    output_chain_id,
    output_asset_id
)
DO UPDATE SET
    sample_count = EXCLUDED.sample_count,
    total_duration_ms = EXCLUDED.total_duration_ms,
    avg_duration_ms = EXCLUDED.avg_duration_ms,
    min_duration_ms = EXCLUDED.min_duration_ms,
    max_duration_ms = EXCLUDED.max_duration_ms,
    last_sample_at = EXCLUDED.last_sample_at,
    updated_at = now();

CREATE OR REPLACE FUNCTION public.router_record_swap_time_average()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    duration_ms bigint;
BEGIN
    IF NEW.status <> 'completed'
        OR NEW.transition_decl_id IS NULL
        OR NEW.started_at IS NULL
        OR NEW.completed_at IS NULL
        OR NEW.completed_at < NEW.started_at
    THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE'
        AND OLD.status = 'completed'
        AND OLD.transition_decl_id IS NOT NULL
        AND OLD.started_at IS NOT NULL
        AND OLD.completed_at IS NOT NULL
        AND OLD.completed_at >= OLD.started_at
    THEN
        RETURN NEW;
    END IF;

    duration_ms := ROUND(EXTRACT(EPOCH FROM (NEW.completed_at - NEW.started_at)) * 1000)::bigint;

    INSERT INTO public.router_swap_time_averages (
        provider,
        leg_type,
        transition_decl_id,
        input_chain_id,
        input_asset_id,
        output_chain_id,
        output_asset_id,
        sample_count,
        total_duration_ms,
        avg_duration_ms,
        min_duration_ms,
        max_duration_ms,
        last_sample_at,
        created_at,
        updated_at
    )
    VALUES (
        NEW.provider,
        NEW.leg_type,
        NEW.transition_decl_id,
        NEW.input_chain_id,
        NEW.input_asset_id,
        NEW.output_chain_id,
        NEW.output_asset_id,
        1,
        duration_ms,
        duration_ms,
        duration_ms,
        duration_ms,
        NEW.completed_at,
        now(),
        now()
    )
    ON CONFLICT (
        provider,
        leg_type,
        transition_decl_id,
        input_chain_id,
        input_asset_id,
        output_chain_id,
        output_asset_id
    )
    DO UPDATE SET
        sample_count = router_swap_time_averages.sample_count + 1,
        total_duration_ms = router_swap_time_averages.total_duration_ms + EXCLUDED.total_duration_ms,
        avg_duration_ms = ROUND(
            (
                router_swap_time_averages.total_duration_ms
                + EXCLUDED.total_duration_ms
            )::numeric
            / (router_swap_time_averages.sample_count + 1)
        )::bigint,
        min_duration_ms = LEAST(
            router_swap_time_averages.min_duration_ms,
            EXCLUDED.min_duration_ms
        ),
        max_duration_ms = GREATEST(
            router_swap_time_averages.max_duration_ms,
            EXCLUDED.max_duration_ms
        ),
        last_sample_at = GREATEST(
            router_swap_time_averages.last_sample_at,
            EXCLUDED.last_sample_at
        ),
        updated_at = now();

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS router_record_swap_time_average ON public.order_execution_legs;
CREATE TRIGGER router_record_swap_time_average
AFTER INSERT OR UPDATE OF
    status,
    transition_decl_id,
    started_at,
    completed_at,
    provider,
    leg_type,
    input_chain_id,
    input_asset_id,
    output_chain_id,
    output_asset_id
ON public.order_execution_legs
FOR EACH ROW EXECUTE FUNCTION public.router_record_swap_time_average();

ALTER TABLE public.market_order_quotes
    ADD COLUMN expected_swap_time_ms bigint;

ALTER TABLE public.market_order_quotes
    ADD CONSTRAINT market_order_quotes_expected_swap_time_ms_nonnegative
    CHECK (expected_swap_time_ms IS NULL OR expected_swap_time_ms >= 0);

ALTER TABLE public.limit_order_quotes
    ADD COLUMN expected_swap_time_ms bigint;

ALTER TABLE public.limit_order_quotes
    ADD CONSTRAINT limit_order_quotes_expected_swap_time_ms_nonnegative
    CHECK (expected_swap_time_ms IS NULL OR expected_swap_time_ms >= 0);
