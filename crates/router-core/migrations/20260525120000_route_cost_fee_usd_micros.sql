ALTER TABLE public.router_route_cost_snapshots
    ADD COLUMN estimated_fee_usd_micros bigint;

UPDATE public.router_route_cost_snapshots
SET estimated_fee_usd_micros = CASE
    WHEN estimated_fee_bps = 0 THEN 0
    ELSE (((sample_amount_usd_micros::numeric * estimated_fee_bps::numeric) + 9999) / 10000)::bigint
END;

ALTER TABLE public.router_route_cost_snapshots
    ALTER COLUMN estimated_fee_usd_micros SET NOT NULL;

ALTER TABLE public.router_route_cost_snapshots
    ADD CONSTRAINT router_route_cost_snapshots_estimated_fee_usd_micros_check
        CHECK (estimated_fee_usd_micros >= 0);
